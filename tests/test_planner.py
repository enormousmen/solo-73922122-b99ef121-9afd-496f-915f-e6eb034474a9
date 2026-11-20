"""试算引擎与 HTTP 接口的自测。

直接对 samples/ 下的四组快照以及手工构造的边界用例做断言。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import (
    HOSTNAME_TOPOLOGY_KEY,
    parse_cpu_millicores,
    parse_memory_bytes,
    parse_nodes,
    parse_pods,
)
from app.planner import (
    MAX_DRAIN_NODES,
    MAX_MIGRATING_PODS,
    Planner,
    run_plan,
)

SAMPLES = Path(__file__).resolve().parent.parent / "samples"
client = TestClient(app)


def load_scenario(name: str):
    base = SAMPLES / name
    nodes = parse_nodes(json.loads((base / "nodes.json").read_text()))
    pods = parse_pods(json.loads((base / "pods.json").read_text()))
    drains = json.loads((base / "drained-nodes.json").read_text())
    return nodes, pods, drains


# --------------------------------------------------------------------------- #
# 资源量解析
# --------------------------------------------------------------------------- #
def test_cpu_parsing():
    assert parse_cpu_millicores("250m") == 250
    assert parse_cpu_millicores("2") == 2000
    assert parse_cpu_millicores(1.5) == 1500
    assert parse_cpu_millicores(None) == 0
    with pytest.raises(ValueError):
        parse_cpu_millicores(True)


def test_memory_parsing():
    assert parse_memory_bytes("1Gi") == 1024 ** 3
    assert parse_memory_bytes("512Mi") == 512 * 1024 ** 2
    assert parse_memory_bytes("1G") == 10 ** 9
    assert parse_memory_bytes(2048) == 2048
    assert parse_memory_bytes(None) == 0
    with pytest.raises(ValueError):
        parse_memory_bytes("oops")


# --------------------------------------------------------------------------- #
# 引擎：可迁移场景
# --------------------------------------------------------------------------- #
def test_feasible_scenario_has_full_plan():
    nodes, pods, drains = load_scenario("feasible")
    result = run_plan(nodes, pods, drains)

    assert result.feasible is True
    assert result.violations == []
    # 4 个普通待迁 Pod；DaemonSet Pod 被排除
    assert sorted(result.migrating_pods) == [
        "team1/cache-1", "team1/web-1", "team1/web-2", "team1/worker-1",
    ]
    assert [e["pod"] for e in result.excluded_pods] == [
        "kube-system/fluentd-node-a"
    ]

    placement = {p["pod"]: p["to_node"] for p in result.placement}
    # nodeSelector：worker-1 只能去 workload=app 的节点（node-d 是 batch）
    assert placement["team1/worker-1"] != "node-d"
    # 两个 web 带 hostname 硬反亲和，不能落到同一节点
    assert placement["team1/web-1"] != placement["team1/web-2"]
    # node-c 已有 redis-0；web 的反亲和选择器是 app=web，不与 redis 冲突
    # 所有落点必须是承接节点
    assert set(placement.values()) <= {"node-c", "node-d", "node-e"}

    # 余量非负，且余量 + 已用 == 可分配
    for r in result.node_remaining:
        assert r["free_cpu_m"] >= 0 and r["free_mem_bytes"] >= 0
        assert r["free_cpu_m"] + r["used_cpu_m"] == r["cpu_alloc_m"]
        assert r["free_mem_bytes"] + r["used_mem_bytes"] == r["mem_alloc_bytes"]


def test_ended_pod_does_not_consume_capacity():
    nodes, pods, drains = load_scenario("feasible")
    result = run_plan(nodes, pods, drains)
    node_c = next(c for c in result.candidates if c["node"] == "node-c")
    # redis-0 占 1000m；Succeeded 的 job-done（2000m）不占
    assert node_c["used_cpu_m"] == 1000
    assert node_c["free_cpu_m"] == 3000


# --------------------------------------------------------------------------- #
# 引擎：不可迁移 / 不支持场景
# --------------------------------------------------------------------------- #
def test_infeasible_resource_reports_blockers():
    nodes, pods, drains = load_scenario("infeasible-resource")
    result = run_plan(nodes, pods, drains)
    assert result.feasible is False
    assert result.violations
    assert result.placement == []
    diag = {d["pod"]: d for d in result.diagnostics}
    # big-1 需要 2000m，而每个承接节点只剩 500m
    assert any("CPU 不足" in b for b in diag["team1/big-1"]["blockers"])


def test_infeasible_anti_affinity():
    nodes, pods, drains = load_scenario("infeasible-anti")
    result = run_plan(nodes, pods, drains)
    assert result.feasible is False
    assert result.violations
    # node-e 不 Ready，不能作为承接节点；候选只有 node-c / node-d
    assert sorted(c["node"] for c in result.candidates) == ["node-c", "node-d"]


def test_unsupported_pod_structure_rejected():
    nodes, pods, drains = load_scenario("unsupported")
    result = run_plan(nodes, pods, drains)
    assert result.feasible is False
    joined = " ".join(result.violations)
    assert "legacy-app" in joined and "容器数为 2" in joined
    assert "init-app" in joined and "init 容器" in joined


def test_tainted_node_cannot_be_drained():
    nodes, pods, _ = load_scenario("unsupported")
    result = run_plan(nodes, pods, ["drain-tainted"])
    assert result.feasible is False
    assert any("污点" in v for v in result.violations)


def test_unknown_node_rejected():
    nodes, pods, _ = load_scenario("feasible")
    result = run_plan(nodes, pods, ["ghost-node"])
    assert result.feasible is False
    assert any("不在 NodeList" in v for v in result.violations)


def test_drain_limit_enforced():
    from scripts.generate_samples import node as mk_node, node_list
    nodes, pods, _ = load_scenario("feasible")
    extra = parse_nodes(node_list([
        mk_node("node-f", "4", 4096), mk_node("node-g", "4", 4096),
    ]))
    names = [n.name for n in nodes + extra][: MAX_DRAIN_NODES + 1]
    assert len(names) == MAX_DRAIN_NODES + 1
    result = run_plan(nodes + extra, pods, names)
    assert result.feasible is False
    assert any(f"超过上限 {MAX_DRAIN_NODES} 个" in v for v in result.violations)


def test_migrating_pod_limit_enforced():
    # 手工构造 9 个合规待迁 Pod，超过 8 个上限
    from scripts.generate_samples import node as mk_node, pod as mk_pod, node_list, pod_list
    raw_nodes = node_list([
        mk_node("d1", "8", 8192), mk_node("d2", "8", 8192),
        mk_node("c1", "8", 8192),
    ])
    raw_pods = pod_list([
        mk_pod("ns", f"p{i}", "d1" if i < 5 else "d2", 100, 128,
               labels={"app": f"p{i}"})
        for i in range(MAX_MIGRATING_PODS + 1)
    ])
    result = run_plan(parse_nodes(raw_nodes), parse_pods(raw_pods),
                      ["d1", "d2"])
    assert result.feasible is False
    assert any(f"超过单次上限 {MAX_MIGRATING_PODS}" in v
               for v in result.violations)


# --------------------------------------------------------------------------- #
# 双向反亲和专项
# --------------------------------------------------------------------------- #
def test_reverse_direction_anti_affinity_blocks_placement():
    """B 不声明反亲和，但 A 声明的选择器命中 B：同节点仍须禁止。"""
    from scripts.generate_samples import node as mk_node, pod as mk_pod, node_list, pod_list
    raw_nodes = node_list([mk_node("d1", "4", 4096), mk_node("c1", "4", 4096)])
    raw_pods = pod_list([
        # 已在承接节点上的 B（无反亲和声明）
        mk_pod("ns", "svc-b", "c1", 100, 128, labels={"role": "b"}),
        # 待迁 A：声明禁止 role=b 的 Pod 同节点
        mk_pod("ns", "svc-a", "d1", 100, 128,
               labels={"role": "a"}, anti_labels={"role": "b"}),
    ])
    result = run_plan(parse_nodes(raw_nodes), parse_pods(raw_pods), ["d1"])
    assert result.feasible is False
    assert result.diagnostics  # 唯一承接节点被反亲和挡住


def test_anti_affinity_scoped_to_same_namespace():
    """不同命名空间即使标签命中也不构成反亲和冲突。"""
    from scripts.generate_samples import node as mk_node, pod as mk_pod, node_list, pod_list
    raw_nodes = node_list([mk_node("d1", "4", 4096), mk_node("c1", "4", 4096)])
    raw_pods = pod_list([
        mk_pod("other", "svc-b", "c1", 100, 128, labels={"role": "b"}),
        mk_pod("team1", "svc-a", "d1", 100, 128,
               labels={"role": "a"}, anti_labels={"role": "b"}),
    ])
    result = run_plan(parse_nodes(raw_nodes), parse_pods(raw_pods), ["d1"])
    assert result.feasible is True
    assert result.placement[0]["to_node"] == "c1"


def test_unsupported_affinity_constraints_flagged():
    from scripts.generate_samples import node as mk_node, pod as mk_pod, node_list, pod_list
    raw_nodes = node_list([mk_node("d1", "4", 4096), mk_node("c1", "4", 4096)])
    p = mk_pod("ns", "p1", "d1", 100, 128, labels={"app": "x"})
    p["spec"]["affinity"] = {
        "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {"topologyKey": "failure-domain.beta.kubernetes.io/zone",
                 "labelSelector": {"matchLabels": {"app": "x"}}}
            ]
        }
    }
    pods = parse_pods(pod_list([p]))
    assert any("拓扑键" in r for r in pods[0].unsupported_reasons)

    # matchExpressions 同样不支持
    p2 = mk_pod("ns", "p2", "d1", 100, 128, labels={"app": "x"})
    p2["spec"]["affinity"] = {
        "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [
                {"topologyKey": HOSTNAME_TOPOLOGY_KEY,
                 "labelSelector": {"matchExpressions": [
                     {"key": "app", "operator": "In", "values": ["x"]}]}}
            ]
        }
    }
    pods2 = parse_pods(pod_list([p2]))
    assert any("matchExpressions" in r for r in pods2[0].unsupported_reasons)


# --------------------------------------------------------------------------- #
# HTTP 接口
# --------------------------------------------------------------------------- #
def _post_plan(scenario: str):
    base = SAMPLES / scenario
    with open(base / "nodes.json", "rb") as nf, open(base / "pods.json", "rb") as pf:
        return client.post(
            "/api/plan",
            files={"node_list": ("nodes.json", nf, "application/json"),
                   "pod_list": ("pods.json", pf, "application/json")},
            data={"drained_nodes": (base / "drained-nodes.json").read_text()},
        )


def test_http_feasible():
    resp = _post_plan("feasible")
    assert resp.status_code == 200
    data = resp.json()
    assert data["feasible"] is True
    assert len(data["placement"]) == 4


def test_http_infeasible_and_health():
    resp = _post_plan("infeasible-anti")
    assert resp.status_code == 200
    data = resp.json()
    assert data["feasible"] is False and data["violations"]
    assert client.get("/healthz").json() == {"status": "ok"}


def test_http_overview_lists_eligibility():
    base = SAMPLES / "unsupported"
    with open(base / "nodes.json", "rb") as nf, open(base / "pods.json", "rb") as pf:
        resp = client.post(
            "/api/overview",
            files={"node_list": ("nodes.json", nf, "application/json"),
                   "pod_list": ("pods.json", pf, "application/json")},
        )
    assert resp.status_code == 200
    nodes = {n["name"]: n for n in resp.json()["nodes"]}
    assert nodes["drain-tainted"]["eligible"] is False
    assert any("污点" in r for r in nodes["drain-tainted"]["ineligible_reasons"])
    assert nodes["node-c"]["eligible"] is True


def test_http_bad_json_rejected():
    resp = client.post(
        "/api/plan",
        files={"node_list": ("n.json", b"{not json", "application/json"),
               "pod_list": ("p.json", b'{"items":[]}', "application/json")},
        data={"drained_nodes": "[]"},
    )
    assert resp.status_code == 400
    assert "不是合法 JSON" in resp.json()["detail"]


def test_http_plan_json_endpoint():
    base = SAMPLES / "feasible"
    body = {
        "node_list": json.loads((base / "nodes.json").read_text()),
        "pod_list": json.loads((base / "pods.json").read_text()),
        "drained_nodes": json.loads((base / "drained-nodes.json").read_text()),
    }
    resp = client.post("/api/plan-json", json=body)
    assert resp.status_code == 200
    assert resp.json()["feasible"] is True


def test_index_page_served():
    resp = client.get("/")
    assert resp.status_code == 200
    assert "缩容" in resp.text


def test_remaining_capacity_matches_full_accounting():
    """承接节点余量 = allocatable - 常驻未结束 Pod requests - 迁入 Pod requests。"""
    nodes, pods, drains = load_scenario("feasible")
    result = run_plan(nodes, pods, drains)
    assert result.feasible
    pod_by_ref = {(p.namespace, p.name): p for p in pods}
    node_by_name = {n.name: n for n in nodes}

    for row in result.node_remaining:
        node = node_by_name[row["node"]]
        resident_cpu = sum(
            p.cpu_request_m for p in pods
            if p.node == node.name and p.active
            and p.ref not in result.migrating_pods
            and p.ref not in [e["pod"] for e in result.excluded_pods]
        )
        incoming_cpu = sum(
            pod_by_ref[tuple(r.split("/", 1))].cpu_request_m
            for r in row["incoming_pods"]
        )
        assert row["used_cpu_m"] == resident_cpu + incoming_cpu
        assert row["free_cpu_m"] == node.cpu_alloc_m - resident_cpu - incoming_cpu
        resident_mem = sum(
            p.mem_request_bytes for p in pods
            if p.node == node.name and p.active
            and p.ref not in result.migrating_pods
            and p.ref not in [e["pod"] for e in result.excluded_pods]
        )
        incoming_mem = sum(
            pod_by_ref[tuple(r.split("/", 1))].mem_request_bytes
            for r in row["incoming_pods"]
        )
        assert row["used_mem_bytes"] == resident_mem + incoming_mem
        assert row["free_mem_bytes"] == node.mem_alloc_bytes - resident_mem - incoming_mem
