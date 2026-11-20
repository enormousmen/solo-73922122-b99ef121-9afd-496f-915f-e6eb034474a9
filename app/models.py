"""解析 NodeList / PodList 快照，提取试算所需的领域模型。

只依赖标准库，所有输入均视为不可信数据，解析阶段不做任何导入执行。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

HOSTNAME_TOPOLOGY_KEY = "kubernetes.io/hostname"
ENDED_PHASES = {"Succeeded", "Failed"}


# --------------------------------------------------------------------------- #
# 资源量解析
# --------------------------------------------------------------------------- #
def parse_cpu_millicores(value: Any) -> int:
    """把 Kubernetes CPU 量解析为毫核（millicore）整数。"""
    if value is None:
        return 0
    if isinstance(value, bool):  # bool 是 int 的子类，显式拒绝
        raise ValueError(f"非法 CPU 量: {value!r}")
    if isinstance(value, (int, float)):
        return int(round(float(value) * 1000))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        if text.endswith("m"):
            return int(round(float(text[:-1])))
        return int(round(float(text) * 1000))
    raise ValueError(f"非法 CPU 量: {value!r}")


_MEMORY_SUFFIXES = {
    "Ki": 1024, "Mi": 1024 ** 2, "Gi": 1024 ** 3,
    "Ti": 1024 ** 4, "Pi": 1024 ** 5, "Ei": 1024 ** 6,
    "K": 10 ** 3, "M": 10 ** 6, "G": 10 ** 9,
    "T": 10 ** 12, "P": 10 ** 15, "E": 10 ** 18,
}


def parse_memory_bytes(value: Any) -> int:
    """把 Kubernetes 内存量解析为字节整数。"""
    if value is None:
        return 0
    if isinstance(value, bool):
        raise ValueError(f"非法内存量: {value!r}")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0
        for suffix, factor in _MEMORY_SUFFIXES.items():
            if text.endswith(suffix):
                return int(round(float(text[: -len(suffix)]) * factor))
        return int(float(text))
    raise ValueError(f"非法内存量: {value!r}")


def _items(payload: Any) -> list[dict]:
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return [x for x in payload["items"] if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    raise ValueError("快照既不是 Kubernetes List 对象（含 items），也不是数组")


# --------------------------------------------------------------------------- #
# 领域模型
# --------------------------------------------------------------------------- #
@dataclass
class NodeInfo:
    name: str
    labels: dict[str, str]
    taints: list[dict]
    ready: bool
    unschedulable: bool
    cpu_alloc_m: int
    mem_alloc_bytes: int
    ineligible_reasons: list[str] = field(default_factory=list)


@dataclass
class AntiSelector:
    """硬反亲和中的一条 matchLabels 选择器（拓扑键已限定为 hostname）。"""
    match_labels: dict[str, str]


@dataclass
class PodInfo:
    namespace: str
    name: str
    node: str
    phase: str
    active: bool
    labels: dict[str, str]
    cpu_request_m: int
    mem_request_bytes: int
    node_selector: dict[str, str]
    anti_selectors: list[AntiSelector]
    unsupported_reasons: list[str] = field(default_factory=list)
    container_count: int = 0
    init_container_count: int = 0
    volume_count: int = 0
    is_daemonset: bool = False
    is_mirror: bool = False

    @property
    def ref(self) -> str:
        return f"{self.namespace}/{self.name}"


def parse_nodes(payload: Any) -> list[NodeInfo]:
    nodes: list[NodeInfo] = []
    for raw in _items(payload):
        metadata = raw.get("metadata") or {}
        spec = raw.get("spec") or {}
        status = raw.get("status") or {}
        name = metadata.get("name") or "<unknown>"
        labels = {k: str(v) for k, v in (metadata.get("labels") or {}).items()}

        taints = spec.get("taints") or []
        unschedulable = bool(spec.get("unschedulable"))

        ready = False
        for cond in status.get("conditions") or []:
            if cond.get("type") == "Ready":
                ready = cond.get("status") == "True"

        allocatable = status.get("allocatable") or {}
        reasons: list[str] = []
        try:
            cpu_alloc = parse_cpu_millicores(allocatable.get("cpu"))
        except ValueError as exc:
            cpu_alloc = 0
            reasons.append(str(exc))
        try:
            mem_alloc = parse_memory_bytes(allocatable.get("memory"))
        except ValueError as exc:
            mem_alloc = 0
            reasons.append(str(exc))
        if "cpu" not in allocatable:
            reasons.append("节点缺少 status.allocatable.cpu")
        if "memory" not in allocatable:
            reasons.append("节点缺少 status.allocatable.memory")

        nodes.append(NodeInfo(
            name=name,
            labels=labels,
            taints=list(taints),
            ready=ready,
            unschedulable=unschedulable,
            cpu_alloc_m=cpu_alloc,
            mem_alloc_bytes=mem_alloc,
            ineligible_reasons=reasons,
        ))
    return nodes


def _extract_anti_selectors(affinity: dict) -> tuple[list[AntiSelector], list[str]]:
    """提取 hostname 拓扑键的 matchLabels 硬反亲和，其余硬约束报为不支持。"""
    selectors: list[AntiSelector] = []
    unsupported: list[str] = []

    pod_affinity = affinity.get("podAffinity") or {}
    if pod_affinity.get("requiredDuringSchedulingIgnoredDuringExecution"):
        unsupported.append("含硬性 podAffinity（requiredDuringScheduling...），试算器不支持")

    node_affinity = affinity.get("nodeAffinity") or {}
    if node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution"):
        unsupported.append("含硬性 nodeAffinity，试算器仅支持 nodeSelector")

    anti = affinity.get("podAntiAffinity") or {}
    terms = anti.get("requiredDuringSchedulingIgnoredDuringExecution") or []
    for term in terms:
        topology_key = term.get("topologyKey")
        if topology_key != HOSTNAME_TOPOLOGY_KEY:
            unsupported.append(
                f"硬反亲和拓扑键为 {topology_key!r}，仅支持 {HOSTNAME_TOPOLOGY_KEY}"
            )
            continue
        selector = term.get("labelSelector") or {}
        if selector.get("matchExpressions"):
            unsupported.append("硬反亲和 labelSelector 含 matchExpressions，仅支持 matchLabels")
            continue
        match_labels = selector.get("matchLabels") or {}
        if match_labels:
            selectors.append(AntiSelector({k: str(v) for k, v in match_labels.items()}))
    # preferredDuringScheduling* 属于软约束，不影响硬性可调度性，忽略。
    return selectors, unsupported


def parse_pods(payload: Any) -> list[PodInfo]:
    pods: list[PodInfo] = []
    for raw in _items(payload):
        metadata = raw.get("metadata") or {}
        spec = raw.get("spec") or {}
        status = raw.get("status") or {}

        namespace = metadata.get("namespace") or "default"
        name = metadata.get("name") or "<unknown>"
        labels = {k: str(v) for k, v in (metadata.get("labels") or {}).items()}
        phase = status.get("phase") or "Pending"
        active = phase not in ENDED_PHASES

        containers = spec.get("containers") or []
        init_containers = spec.get("initContainers") or []
        volumes = spec.get("volumes") or []
        node_selector = {k: str(v) for k, v in (spec.get("nodeSelector") or {}).items()}

        unsupported: list[str] = []
        if len(containers) != 1:
            unsupported.append(f"容器数为 {len(containers)}，仅支持单容器 Pod")
        if init_containers:
            unsupported.append(f"含 {len(init_containers)} 个 init 容器，仅支持无初始化容器的 Pod")
        if volumes:
            unsupported.append(f"含 {len(volumes)} 个卷，仅支持无卷 Pod")

        anti_selectors: list[AntiSelector] = []
        affinity = spec.get("affinity")
        if isinstance(affinity, dict):
            anti_selectors, affinity_reasons = _extract_anti_selectors(affinity)
            unsupported.extend(affinity_reasons)

        cpu_req = mem_req = 0
        if len(containers) == 1:
            requests = (containers[0].get("resources") or {}).get("requests") or {}
            try:
                cpu_req = parse_cpu_millicores(requests.get("cpu"))
            except ValueError as exc:
                unsupported.append(str(exc))
            try:
                mem_req = parse_memory_bytes(requests.get("memory"))
            except ValueError as exc:
                unsupported.append(str(exc))

        owner_refs = metadata.get("ownerReferences") or []
        is_daemonset = any(o.get("kind") == "DaemonSet" for o in owner_refs)
        annotations = metadata.get("annotations") or {}
        is_mirror = "kubernetes.io/config.mirror" in annotations

        pods.append(PodInfo(
            namespace=namespace,
            name=name,
            node=spec.get("nodeName") or "",
            phase=phase,
            active=active,
            labels=labels,
            cpu_request_m=cpu_req,
            mem_request_bytes=mem_req,
            node_selector=node_selector,
            anti_selectors=anti_selectors,
            unsupported_reasons=unsupported,
            container_count=len(containers),
            init_container_count=len(init_containers),
            volume_count=len(volumes),
            is_daemonset=is_daemonset,
            is_mirror=is_mirror,
        ))
    return pods
