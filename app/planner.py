"""缩容落点试算引擎。

规则：
- 仅允许勾选无污点、Ready、可调度且可分配量完整的节点作为下线节点；
- 承接节点是其余同样无污点、Ready、可调度且可分配量完整的节点；
- 已结束（Succeeded/Failed）的 Pod 不占资源，其余在承接节点上的 Pod
  的 requests 从 allocatable 中扣除；
- 所有待迁 Pod 必须在同一份方案中同时放下，且满足 nodeSelector；
- 双向检查同命名空间内、以 kubernetes.io/hostname 为拓扑键的
  matchLabels 硬反亲和；
- 常驻 Pod（未下线节点上尚未结束）的硬反亲和含 matchExpressions 时，
  表达式不求值：默认匹配同命名空间、拓扑键为 hostname 的条款会把
  同命名空间待迁 Pod 落入该拓扑域（同一节点）的落点标为待核验。
  第一轮严格避开待核验落点，找到完整方案照常展示；只有放开待核验
  落点才有完整方案时判定“无法确认”，点名一份候选方案涉及的常驻 Pod
  与拓扑域并撤下落点；放开后仍无解则报告无完整方案。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import HOSTNAME_TOPOLOGY_KEY, NodeInfo, PodInfo

MAX_DRAIN_NODES = 6
MAX_MIGRATING_PODS = 8


@dataclass
class Candidate:
    node: NodeInfo
    used_cpu_m: int
    used_mem_bytes: int

    @property
    def free_cpu_m(self) -> int:
        return self.node.cpu_alloc_m - self.used_cpu_m

    @property
    def free_mem_bytes(self) -> int:
        return self.node.mem_alloc_bytes - self.used_mem_bytes


@dataclass
class PlanResult:
    feasible: bool
    drained_nodes: list[str]
    status: str = "infeasible"  # feasible / unverifiable / infeasible
    excluded_pods: list[dict] = field(default_factory=list)
    migrating_pods: list[str] = field(default_factory=list)
    candidates: list[dict] = field(default_factory=list)
    placement: list[dict] = field(default_factory=list)
    node_remaining: list[dict] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)
    diagnostics: list[dict] = field(default_factory=list)
    pending_verification: list[dict] = field(default_factory=list)


def _node_eligibility(node: NodeInfo) -> list[str]:
    reasons = list(node.ineligible_reasons)
    if node.taints:
        reasons.append(f"节点带 {len(node.taints)} 个污点")
    if not node.ready:
        reasons.append("节点未处于 Ready 状态")
    if node.unschedulable:
        reasons.append("节点被标记为不可调度（cordoned）")
    return reasons


def _selector_matches(labels: dict[str, str], match_labels: dict[str, str]) -> bool:
    return all(labels.get(k) == v for k, v in match_labels.items())


class Planner:
    def __init__(self, nodes: list[NodeInfo], pods: list[PodInfo]):
        self.nodes = {n.name: n for n in nodes}
        self.pods = pods
        self.errors: list[str] = []
        self._migrating: list[PodInfo] = []

    def plan(self, drained_names: list[str]) -> PlanResult:
        drained = self._validate_drain_selection(drained_names)
        if self.errors:
            return PlanResult(
                feasible=False,
                drained_nodes=list(drained_names),
                violations=list(self.errors),
            )

        # 1. 收集下线节点上的活动 Pod，剔除 DaemonSet/静态 Pod（drain 本就不迁它们）
        excluded: list[dict] = []
        migrating: list[PodInfo] = []
        for pod in self.pods:
            if pod.node not in drained or not pod.active:
                continue
            if pod.is_daemonset or pod.is_mirror:
                excluded.append({
                    "pod": pod.ref,
                    "node": pod.node,
                    "reason": "DaemonSet 或静态/mirror Pod，drain 不迁移（会由控制器在承接节点重建）",
                })
                continue
            migrating.append(pod)

        if len(migrating) > MAX_MIGRATING_PODS:
            self.errors.append(
                f"待迁 Pod 共 {len(migrating)} 个，超过单次上限 {MAX_MIGRATING_PODS} 个"
            )
            return PlanResult(
                feasible=False,
                drained_nodes=list(drained_names),
                excluded_pods=excluded,
                violations=list(self.errors),
            )

        unsupported = [p for p in migrating if p.unsupported_reasons]
        for pod in unsupported:
            self.errors.append(
                f"待迁 Pod {pod.ref} 含不支持的约束/结构："
                + "；".join(pod.unsupported_reasons)
            )
        if self.errors:
            return PlanResult(
                feasible=False,
                drained_nodes=list(drained_names),
                excluded_pods=excluded,
                migrating_pods=[p.ref for p in migrating],
                violations=list(self.errors),
            )
        self._migrating = migrating

        # 2. 构建承接节点，扣除仍在该节点且尚未结束的 Pod 请求量
        candidates = self._build_candidates(drained, migrating, excluded)

        candidate_payload = [
            {
                "node": c.node.name,
                "cpu_alloc_m": c.node.cpu_alloc_m,
                "mem_alloc_bytes": c.node.mem_alloc_bytes,
                "used_cpu_m": c.used_cpu_m,
                "used_mem_bytes": c.used_mem_bytes,
                "free_cpu_m": c.free_cpu_m,
                "free_mem_bytes": c.free_mem_bytes,
            }
            for c in candidates
        ]

        base = PlanResult(
            feasible=False,
            drained_nodes=list(drained_names),
            excluded_pods=excluded,
            migrating_pods=[p.ref for p in sorted(migrating, key=lambda p: p.name)],
            candidates=candidate_payload,
        )

        if not migrating:
            # 没有需要迁移的普通 Pod，空方案本身就是完整方案
            base.feasible = True
            base.status = "feasible"
            base.node_remaining = self._remaining_payload(candidates, {})
            return base

        # 3. 第一轮：严格避开待核验落点（常驻 Pod 的 matchExpressions 硬反亲和）
        strict_map = self._feasible_map(migrating, candidates)
        diagnostics: list[dict] = []
        for pod in sorted(migrating, key=lambda p: p.name):
            if not strict_map[pod.ref]:
                diagnostics.append(
                    {"pod": pod.ref, "blockers": self._blocker_reasons(pod, candidates)}
                )
        base.diagnostics = diagnostics

        strict_assignment: dict[str, str] | None = None
        ordered: list[PodInfo] = []
        if not diagnostics:
            ordered = sorted(migrating, key=lambda p: (len(strict_map[p.ref]), p.name))
            assignment: dict[str, str] = {}
            if self._backtrack(ordered, 0, candidates, strict_map, assignment):
                strict_assignment = assignment

        if strict_assignment is not None:
            base.feasible = True
            base.status = "feasible"
            base.placement = [
                {
                    "pod": pod.ref,
                    "from_node": pod.node,
                    "to_node": strict_assignment[pod.ref],
                    "cpu_request_m": pod.cpu_request_m,
                    "mem_request_bytes": pod.mem_request_bytes,
                }
                for pod in sorted(migrating, key=lambda p: p.name)
            ]
            base.node_remaining = self._remaining_payload(candidates, strict_assignment)
            return base

        # 4. 严格模式失败：没有待核验条款可放开时，按原有逻辑判定无完整方案
        if not self._unverifiable_in_play(candidates, migrating):
            if diagnostics:
                base.violations.append("存在对任何承接节点都不可调度的待迁 Pod，无完整方案")
            else:
                base.violations.append(
                    "反亲和/资源约束冲突：不存在让全部待迁 Pod 同时落地的完整方案"
                )
                base.diagnostics.extend(
                    self._explain_backtrack_failure(ordered, candidates, strict_map)
                )
            return base

        # 5. 第二轮：放开待核验落点再试，验证“无法确认”与“无完整方案”的边界
        relaxed_map = self._feasible_map(migrating, candidates, allow_unverifiable=True)
        relaxed_assignment: dict[str, str] | None = None
        if all(relaxed_map[p.ref] for p in migrating):
            relaxed_ordered = sorted(
                migrating, key=lambda p: (len(relaxed_map[p.ref]), p.name)
            )
            assignment = {}
            if self._backtrack(relaxed_ordered, 0, candidates, relaxed_map,
                               assignment, allow_unverifiable=True):
                relaxed_assignment = assignment

        if relaxed_assignment is None:
            base.violations.append(
                "放开待核验落点后仍不存在让全部待迁 Pod 同时落地的完整方案"
            )
            if not diagnostics:
                base.diagnostics.extend(
                    self._explain_backtrack_failure(
                        sorted(migrating,
                               key=lambda p: (len(relaxed_map[p.ref]), p.name)),
                        candidates, relaxed_map,
                    )
                )
            return base

        # 只有放开待核验落点才有完整方案：无法确认，点名涉及的常驻 Pod 与拓扑域
        base.status = "unverifiable"
        base.pending_verification = self._pending_payload(relaxed_assignment)
        base.violations.append(
            "只有放开待核验落点才有完整方案：常驻 Pod 的 matchExpressions 硬反亲和"
            "未参与求值，落点无法确认，已撤下旧方案"
        )
        return base

    # ------------------------------------------------------------------ #
    def _validate_drain_selection(self, drained_names: list[str]) -> set[str]:
        deduped = list(dict.fromkeys(drained_names))
        if len(deduped) > MAX_DRAIN_NODES:
            self.errors.append(f"勾选下线节点 {len(deduped)} 个，超过上限 {MAX_DRAIN_NODES} 个")
        drained: set[str] = set()
        for name in deduped:
            node = self.nodes.get(name)
            if node is None:
                self.errors.append(f"节点 {name} 不在 NodeList 快照中")
                continue
            reasons = _node_eligibility(node)
            if reasons:
                self.errors.append(f"下线节点 {name} 不合格：{'；'.join(reasons)}")
                continue
            drained.add(name)
        return drained

    def _build_candidates(
        self,
        drained: set[str],
        migrating: list[PodInfo],
        excluded: list[dict],
    ) -> list[Candidate]:
        migrating_keys = {(p.namespace, p.name) for p in migrating}
        candidates: list[Candidate] = []
        for node in sorted(self.nodes.values(), key=lambda n: n.name):
            if node.name in drained:
                continue
            reasons = _node_eligibility(node)
            if reasons:
                continue  # 只在其余就绪且可调度的无污点节点上找落点
            used_cpu = used_mem = 0
            for pod in self.pods:
                if pod.node != node.name or not pod.active:
                    continue
                if (pod.namespace, pod.name) in migrating_keys:
                    continue  # 待迁 Pod 不会留在原承接节点
                used_cpu += pod.cpu_request_m
                used_mem += pod.mem_request_bytes
            candidates.append(Candidate(node=node, used_cpu_m=used_cpu,
                                        used_mem_bytes=used_mem))
        return candidates

    def _feasible_map(
        self,
        migrating: list[PodInfo],
        candidates: list[Candidate],
        allow_unverifiable: bool = False,
    ) -> dict[str, list[str]]:
        """每个待迁 Pod 的可行承接节点（严格模式默认避开待核验落点）。"""
        return {
            pod.ref: [
                c.node.name for c in candidates
                if self._can_place(pod, c, {}, allow_unverifiable)
            ]
            for pod in sorted(migrating, key=lambda p: p.name)
        }

    def _unverifiable_conflicts(self, pod: PodInfo, node_name: str) -> list[PodInfo]:
        """位于该节点、同命名空间且带未核验反亲和条款的常驻 Pod。

        条款默认匹配同命名空间、拓扑键为 hostname，因此待迁 Pod 落入
        该节点即落入其拓扑域；matchExpressions 不求值，只能标为待核验。
        """
        return [
            other for other in self.pods
            if other.ref != pod.ref
            and other.unverifiable_anti_terms
            and other.active
            and other.namespace == pod.namespace
            and other.node == node_name
        ]

    def _unverifiable_in_play(
        self, candidates: list[Candidate], migrating: list[PodInfo]
    ) -> bool:
        """承接节点上是否存在影响本批待迁 Pod 的待核验反亲和条款。"""
        candidate_nodes = {c.node.name for c in candidates}
        namespaces = {p.namespace for p in migrating}
        migrating_keys = {(p.namespace, p.name) for p in migrating}
        return any(
            pod.unverifiable_anti_terms
            and pod.active
            and pod.namespace in namespaces
            and pod.node in candidate_nodes
            and (pod.namespace, pod.name) not in migrating_keys
            for pod in self.pods
        )

    def _pending_payload(self, assignment: dict[str, str]) -> list[dict]:
        """一份候选方案实际用到的待核验落点：点名常驻 Pod 与拓扑域。"""
        pending: list[dict] = []
        for pod in sorted(self._migrating, key=lambda p: p.name):
            node_name = assignment[pod.ref]
            node = self.nodes[node_name]
            for resident in self._unverifiable_conflicts(pod, node_name):
                pending.append({
                    "pod": pod.ref,
                    "node": node_name,
                    "resident_pod": resident.ref,
                    "topology_key": HOSTNAME_TOPOLOGY_KEY,
                    "topology_value": node.labels.get(HOSTNAME_TOPOLOGY_KEY, node_name),
                    "expressions": [
                        expr
                        for term in resident.unverifiable_anti_terms
                        for expr in term.expressions
                    ],
                })
        return pending

    def _can_place(
        self,
        pod: PodInfo,
        candidate: Candidate,
        assignment: dict[str, str],
        allow_unverifiable: bool = False,
    ) -> bool:
        node = candidate.node
        # nodeSelector
        if not _selector_matches(node.labels, pod.node_selector):
            return False
        # 资源（含同一方案中已分配到该节点的待迁 Pod）
        planned_cpu = sum(
            p.cpu_request_m
            for p in self._assigned_pods(assignment)
            if assignment[p.ref] == node.name
        )
        planned_mem = sum(
            p.mem_request_bytes
            for p in self._assigned_pods(assignment)
            if assignment[p.ref] == node.name
        )
        if candidate.used_cpu_m + planned_cpu + pod.cpu_request_m > node.cpu_alloc_m:
            return False
        if candidate.used_mem_bytes + planned_mem + pod.mem_request_bytes > node.mem_alloc_bytes:
            return False
        # 双向硬反亲和（同命名空间、hostname 拓扑键、matchLabels）
        for other in self.pods:
            if other.namespace != pod.namespace or not other.active:
                continue
            other_node = assignment.get(other.ref, other.node)
            if other_node != node.name:
                continue
            if self._anti_conflict(pod, other):
                return False
        # 常驻 Pod 的 matchExpressions 硬反亲和：表达式不求值，落点待核验
        if not allow_unverifiable and self._unverifiable_conflicts(pod, node.name):
            return False
        return True

    def _assigned_pods(self, assignment: dict[str, str]) -> list[PodInfo]:
        keys = set(assignment)
        return [p for p in self.pods if p.ref in keys]

    @staticmethod
    def _anti_conflict(a: PodInfo, b: PodInfo) -> bool:
        if a.ref == b.ref:
            return False
        # a 排斥 b（a 的选择器命中 b 的标签）或 b 排斥 a，任一命中即冲突 —— 双向检查
        for sel in a.anti_selectors:
            if _selector_matches(b.labels, sel.match_labels):
                return True
        for sel in b.anti_selectors:
            if _selector_matches(a.labels, sel.match_labels):
                return True
        return False

    def _backtrack(
        self,
        ordered: list[PodInfo],
        index: int,
        candidates: list[Candidate],
        feasible_map: dict[str, list[str]],
        assignment: dict[str, str],
        allow_unverifiable: bool = False,
    ) -> bool:
        if index == len(ordered):
            return True
        pod = ordered[index]
        for node_name in feasible_map[pod.ref]:
            candidate = self._candidate(candidates, node_name)
            if self._can_place(pod, candidate, assignment, allow_unverifiable):
                assignment[pod.ref] = node_name
                if self._backtrack(ordered, index + 1, candidates,
                                   feasible_map, assignment, allow_unverifiable):
                    return True
                del assignment[pod.ref]
        return False

    @staticmethod
    def _candidate(candidates: list[Candidate], name: str) -> Candidate:
        return next(c for c in candidates if c.node.name == name)

    def _explain_backtrack_failure(
        self,
        ordered: list[PodInfo],
        candidates: list[Candidate],
        feasible_map: dict[str, list[str]],
    ) -> list[dict]:
        """回溯失败后给出每个待迁 Pod 在各候选节点上的静态阻塞原因。"""
        migrating_refs = {p.ref for p in self._migrating}
        diagnostics: list[dict] = []
        for pod in ordered:
            blockers: list[str] = []
            for node_name in feasible_map[pod.ref]:
                candidate = self._candidate(candidates, node_name)
                reasons: list[str] = []
                node = candidate.node
                if candidate.used_cpu_m + pod.cpu_request_m > node.cpu_alloc_m:
                    reasons.append(
                        f"CPU 不足（需 {pod.cpu_request_m}m，余 {candidate.free_cpu_m}m）"
                    )
                if candidate.used_mem_bytes + pod.mem_request_bytes > node.mem_alloc_bytes:
                    reasons.append(
                        f"内存不足（需 {pod.mem_request_bytes}B，余 {candidate.free_mem_bytes}B）"
                    )
                resident_conflicts = [
                    other.ref for other in self.pods
                    if other.namespace == pod.namespace and other.active
                    and other.node == node.name
                    and other.ref not in migrating_refs
                    and self._anti_conflict(pod, other)
                ]
                if resident_conflicts:
                    reasons.append(f"与节点常驻 Pod 硬反亲和冲突：{resident_conflicts}")
                unverifiable = [
                    o.ref for o in self._unverifiable_conflicts(pod, node.name)
                    if o.ref not in migrating_refs
                ]
                if unverifiable:
                    reasons.append(
                        "常驻 Pod 带未核验的 matchExpressions 硬反亲和"
                        f"（落入该拓扑域待核验）：{unverifiable}"
                    )
                peer_conflicts = [
                    other.ref for other in self._migrating
                    if other.ref != pod.ref and self._anti_conflict(pod, other)
                ]
                if peer_conflicts:
                    reasons.append(f"与同批待迁 Pod 硬反亲和，不能同节点：{peer_conflicts}")
                blockers.append(
                    f"{node_name}: {'；'.join(reasons) if reasons else '受其他 Pod 分配组合挤压'}"
                )
            diagnostics.append({"pod": pod.ref, "blockers": blockers})
        return diagnostics

    def _blocker_reasons(self, pod: PodInfo, candidates: list[Candidate]) -> list[str]:
        blockers: list[str] = []
        for candidate in candidates:
            node_reasons: list[str] = []
            if not _selector_matches(candidate.node.labels, pod.node_selector):
                missing = {
                    k: v for k, v in pod.node_selector.items()
                    if candidate.node.labels.get(k) != v
                }
                node_reasons.append(f"nodeSelector 不匹配（缺 {missing}）")
            if candidate.used_cpu_m + pod.cpu_request_m > candidate.node.cpu_alloc_m:
                node_reasons.append(
                    f"CPU 不足（需 {pod.cpu_request_m}m，余 {candidate.free_cpu_m}m）"
                )
            if candidate.used_mem_bytes + pod.mem_request_bytes > candidate.node.mem_alloc_bytes:
                node_reasons.append(
                    f"内存不足（需 {pod.mem_request_bytes}B，余 {candidate.free_mem_bytes}B）"
                )
            conflicts = [
                other.ref for other in self.pods
                if other.namespace == pod.namespace and other.active
                and other.node == candidate.node.name
                and self._anti_conflict(pod, other)
            ]
            if conflicts:
                node_reasons.append(f"硬反亲和冲突：{conflicts}")
            unverifiable = [
                o.ref for o in self._unverifiable_conflicts(pod, candidate.node.name)
            ]
            if unverifiable:
                node_reasons.append(
                    "常驻 Pod 带未核验的 matchExpressions 硬反亲和"
                    f"（落入该拓扑域待核验）：{unverifiable}"
                )
            blockers.append(f"{candidate.node.name}: {'；'.join(node_reasons)}")
        return blockers

    def _remaining_payload(
        self, candidates: list[Candidate], assignment: dict[str, str]
    ) -> list[dict]:
        payload = []
        for candidate in sorted(candidates, key=lambda c: c.node.name):
            name = candidate.node.name
            incoming = [
                p.ref for p in self._assigned_pods(assignment)
                if assignment[p.ref] == name
            ]
            add_cpu = sum(
                p.cpu_request_m for p in self._assigned_pods(assignment)
                if assignment[p.ref] == name
            )
            add_mem = sum(
                p.mem_request_bytes for p in self._assigned_pods(assignment)
                if assignment[p.ref] == name
            )
            payload.append({
                "node": name,
                "cpu_alloc_m": candidate.node.cpu_alloc_m,
                "mem_alloc_bytes": candidate.node.mem_alloc_bytes,
                "used_cpu_m": candidate.used_cpu_m + add_cpu,
                "used_mem_bytes": candidate.used_mem_bytes + add_mem,
                "free_cpu_m": candidate.free_cpu_m - add_cpu,
                "free_mem_bytes": candidate.free_mem_bytes - add_mem,
                "incoming_pods": incoming,
            })
        return payload


def run_plan(nodes: list[NodeInfo], pods: list[PodInfo],
             drained_names: list[str]) -> PlanResult:
    return Planner(nodes, pods).plan(drained_names)
