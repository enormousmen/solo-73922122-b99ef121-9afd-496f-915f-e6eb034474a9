#!/usr/bin/env python3
"""生成试算器的示例 NodeList / PodList 快照（仅用标准库）。

场景：
  feasible/            可迁移：资源 + nodeSelector + hostname 双向反亲和均满足
  infeasible-resource/ 不可迁移：承接节点总资源放不下全部待迁 Pod
  infeasible-anti/     不可迁移：双向硬反亲和导致无完整方案
  unsupported/         不支持的约束：多容器、卷、init 容器、污点节点
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def node(name, cpu, mem_mi, labels=None, taints=None, ready=True,
         unschedulable=False):
    conditions = [{"type": "Ready", "status": "True" if ready else "False",
                   "reason": "KubeletReady"}]
    return {
        "apiVersion": "v1", "kind": "Node",
        "metadata": {"name": name, "labels": {"kubernetes.io/hostname": name,
                                              **(labels or {})}},
        "spec": {
            "taints": taints or [],
            "unschedulable": unschedulable,
        },
        "status": {
            "conditions": conditions,
            "allocatable": {"cpu": cpu, "memory": f"{mem_mi}Mi"},
        },
    }


def pod(namespace, name, node_name, cpu_m, mem_mi, labels=None,
        node_selector=None, anti_labels=None, containers=1,
        init_containers=0, volumes=0, phase="Running", owner=None,
        mirror=False):
    labels = labels or {}
    spec: dict = {
        "nodeName": node_name,
        "containers": [
            {"name": f"{name}-c{i}",
             "resources": {"requests": {"cpu": f"{cpu_m}m",
                                        "memory": f"{mem_mi}Mi"}}}
            for i in range(containers)
        ],
    }
    if node_selector:
        spec["nodeSelector"] = node_selector
    if init_containers:
        spec["initContainers"] = [
            {"name": f"{name}-init{i}", "image": "busybox"}
            for i in range(init_containers)
        ]
    if volumes:
        spec["volumes"] = [{"name": f"vol{i}", "emptyDir": {}}
                           for i in range(volumes)]
    if anti_labels:
        spec["affinity"] = {
            "podAntiAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": [
                    {"topologyKey": "kubernetes.io/hostname",
                     "labelSelector": {"matchLabels": anti_labels}}
                ]
            }
        }
    metadata: dict = {"name": name, "namespace": namespace, "labels": labels}
    if owner:
        kind = owner[0]
        metadata["ownerReferences"] = [
            {"apiVersion": "apps/v1", "kind": kind, "name": owner[1],
             "uid": f"{name}-owner"}
        ]
    if mirror:
        metadata["annotations"] = {"kubernetes.io/config.mirror": "abc123"}
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": metadata, "spec": spec,
        "status": {"phase": phase},
    }


def node_list(items):
    return {"apiVersion": "v1", "kind": "NodeList", "items": items}


def pod_list(items):
    return {"apiVersion": "v1", "kind": "PodList", "items": items}


# --------------------------------------------------------------------------- #
# 场景 1：可迁移
# --------------------------------------------------------------------------- #
def feasible():
    nodes = node_list([
        node("node-a", "4", 4096, {"workload": "app", "zone": "z1"}),
        node("node-b", "4", 4096, {"workload": "app", "zone": "z1"}),
        node("node-c", "4", 4096, {"workload": "app", "zone": "z2"}),
        node("node-d", "4", 4096, {"workload": "batch", "zone": "z2"}),
        node("node-e", "4", 4096, {"workload": "app", "zone": "z3"}),
    ])
    pods = pod_list([
        # 承接节点上的常驻 Pod（占用资源，参与反亲和）
        pod("team1", "redis-0", "node-c", 1000, 1024, {"app": "redis"}),
        pod("team1", "queue-0", "node-d", 500, 512, {"app": "queue"}),
        pod("team1", "metrics-0", "node-e", 500, 512, {"app": "metrics"}),
        # 已结束 Pod：不占资源
        pod("team1", "job-done", "node-c", 2000, 2048, phase="Succeeded"),
        # node-a / node-b 上待迁的 5 个 Pod
        pod("team1", "web-1", "node-a", 500, 512,
            labels={"app": "web", "tier": "frontend"},
            anti_labels={"app": "web"}),
        pod("team1", "web-2", "node-a", 500, 512,
            labels={"app": "web", "tier": "frontend"},
            anti_labels={"app": "web"}),
        pod("team1", "worker-1", "node-a", 1000, 1024,
            labels={"app": "worker"},
            node_selector={"workload": "app"}),
        pod("team1", "cache-1", "node-b", 500, 512,
            labels={"app": "cache"}),
        # DaemonSet Pod：drain 不迁移，应出现在 excluded_pods
        pod("kube-system", "fluentd-node-a", "node-a", 100, 64,
            labels={"app": "fluentd"}, owner=("DaemonSet", "fluentd")),
    ])
    return nodes, pods, ["node-a", "node-b"]


# --------------------------------------------------------------------------- #
# 场景 2：资源放不下（总量超配 + 单节点都放不下的大 Pod）
# --------------------------------------------------------------------------- #
def infeasible_resource():
    nodes = node_list([
        node("drain-1", "4", 4096, {"workload": "app"}),
        node("drain-2", "4", 4096, {"workload": "app"}),
        node("drain-3", "4", 4096, {"workload": "app"}),
        node("node-c", "4", 4096, {"workload": "app"}),
        node("node-d", "4", 4096, {"workload": "app"}),
    ])
    pods = pod_list([
        # 承接节点已占用 3.5C/3.5Gi，每个仅剩 500m/512Mi
        pod("team1", "resident-c", "node-c", 3500, 3584, {"app": "resident"}),
        pod("team1", "resident-d", "node-d", 3500, 3584, {"app": "resident"}),
        pod("team1", "big-1", "drain-1", 2000, 2048, {"app": "big"}),
        pod("team1", "small-1", "drain-2", 500, 512, {"app": "small"}),
        pod("team1", "small-2", "drain-3", 500, 512, {"app": "small"}),
    ])
    return nodes, pods, ["drain-1", "drain-2", "drain-3"]


# --------------------------------------------------------------------------- #
# 场景 3：双向硬反亲和无解
# --------------------------------------------------------------------------- #
def infeasible_anti():
    nodes = node_list([
        node("drain-1", "4", 4096, {"workload": "app"}),
        node("drain-2", "4", 4096, {"workload": "app"}),
        node("node-c", "4", 4096, {"workload": "app"}),
        node("node-d", "4", 4096, {"workload": "app"}),
        # node-e 不 Ready：不能作为承接节点
        node("node-e", "4", 4096, {"workload": "app"}, ready=False),
    ])
    pods = pod_list([
        # node-c 上已有一个 web：web-1 / web-2 都不能去 node-c
        pod("team1", "web-0", "node-c", 200, 256,
            labels={"app": "web"}, anti_labels={"app": "web"}),
        # 两个待迁 web 互相反亲和，且都不能去 node-c；只剩 node-d 一个节点 => 无解
        pod("team1", "web-1", "drain-1", 300, 256,
            labels={"app": "web"}, anti_labels={"app": "web"}),
        pod("team1", "web-2", "drain-2", 300, 256,
            labels={"app": "web"}, anti_labels={"app": "web"}),
    ])
    return nodes, pods, ["drain-1", "drain-2"]


# --------------------------------------------------------------------------- #
# 场景 4：不支持的约束 / 结构
# --------------------------------------------------------------------------- #
def unsupported():
    nodes = node_list([
        node("drain-1", "4", 4096, {"workload": "app"}),
        node("drain-tainted", "4", 4096,
             taints=[{"key": "dedicated", "value": "gpu", "effect": "NoSchedule"}]),
        node("node-c", "4", 4096, {"workload": "app"}),
    ])
    pods = pod_list([
        # 多容器 + 卷：结构不支持
        pod("team1", "legacy-app", "drain-1", 200, 256,
            labels={"app": "legacy"}, containers=2, volumes=1),
        # 带 init 容器：结构不支持
        pod("team1", "init-app", "drain-1", 200, 256,
            labels={"app": "initapp"}, init_containers=1),
        # 普通 Pod：本身合规，但勾选了污点节点时整次请求会被拒绝
        pod("team1", "plain-app", "drain-tainted", 200, 256,
            labels={"app": "plain"}),
    ])
    return nodes, pods, ["drain-1"]


SCENARIOS = {
    "feasible": (feasible, "可迁移：完整方案可求"),
    "infeasible-resource": (infeasible_resource, "不可迁移：资源超配"),
    "infeasible-anti": (infeasible_anti, "不可迁移：反亲和无解"),
    "unsupported": (unsupported, "不支持：多容器/卷/init/污点"),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="生成试算器示例快照")
    parser.add_argument("out_dir", type=Path, help="输出根目录")
    args = parser.parse_args()

    for scenario, (builder, _desc) in SCENARIOS.items():
        nodes, pods, drains = builder()
        out = args.out_dir / scenario
        out.mkdir(parents=True, exist_ok=True)
        (out / "nodes.json").write_text(
            json.dumps(nodes, indent=2, ensure_ascii=False), encoding="utf-8")
        (out / "pods.json").write_text(
            json.dumps(pods, indent=2, ensure_ascii=False), encoding="utf-8")
        (out / "drained-nodes.json").write_text(
            json.dumps(drains, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"generated {scenario}: {len(nodes['items'])} nodes, "
              f"{len(pods['items'])} pods, drains={drains}")


if __name__ == "__main__":
    main()
