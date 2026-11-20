# Kubernetes 缩容落点试算器

值班员在缩容前上传同一批 **NodeList / PodList JSON 快照**，在网页勾选要下线的节点，
系统在其余就绪可调度的节点上为全部待迁 Pod 寻找**同时成立**的落点方案，
并给出逐 Pod 落点与各承接节点余量。无完整方案或遇到不支持的约束时，
明确给出原因并由前端撤下旧方案（不展示任何残留落点）。

## 试算规则

| 主题 | 规则 |
| --- | --- |
| 规模上限 | 单次最多勾选 **6** 个下线节点、最多 **8** 个待迁 Pod |
| 节点资格 | 下线节点与承接节点都必须：无污点、`Ready=True`、未被 cordon、`allocatable` 完整 |
| Pod 结构 | 仅支持**单容器、无 init 容器、无卷**的 Pod；否则报“不支持的约束”并拒绝整次试算 |
| 资源口径 | 从节点 `status.allocatable` 中扣除**仍在该节点且尚未结束**（phase 非 Succeeded/Failed）的 Pod 的 CPU/内存 **requests**；待迁 Pod 不计入原节点占用 |
| 调度约束 | 支持 `nodeSelector`；硬 `nodeAffinity`、硬 `podAffinity` 报不支持 |
| 反亲和 | 仅支持同命名空间内、拓扑键为 `kubernetes.io/hostname`、`labelSelector.matchLabels` 的**硬** `podAntiAffinity`；**双向检查**（A 的选择器命中 B 或 B 的选择器命中 A 都算冲突）；其他拓扑键、`matchExpressions` 报不支持；软约束（preferred）忽略 |
| 完整性 | 所有待迁 Pod 必须在同一份方案中同时放下，采用 MRV + 回溯求解；找不到即判定无完整方案 |
| DaemonSet/静态 Pod | drain 本就不迁移这类 Pod，自动列入 `excluded_pods` 并说明原因 |

## 目录结构

```
app/
  models.py           # 快照解析、CPU/内存量解析、领域模型、约束识别
  planner.py          # 候选筛选、资源核算、双向反亲和、回溯求完整方案
  main.py             # FastAPI 路由（multipart 与 JSON 两种调用方式）
  static/index.html   # 单页前端（上传 → 勾选 → 试算 → 撤旧方案）
samples/              # 4 组可直接试算的快照（由脚本生成）
scripts/generate_samples.py
tests/test_planner.py # 20 个自测用例
Dockerfile / docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`./samples` 以只读方式挂载到容器内 `/data/samples`，可直接在网页上传其中的
`nodes.json` / `pods.json`。

## 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## HTTP 接口

- `POST /api/overview`（multipart：`node_list`、`pod_list`）：返回节点资格与 Pod 概览
- `POST /api/plan`（multipart：两个快照 + 字符串字段 `drained_nodes`，值为 JSON 数组）
- `POST /api/plan-json`（JSON 体：`{node_list, pod_list, drained_nodes}`），适合脚本
- `GET /healthz`

curl 示例（注意 `drained_nodes` 用 `<` 作为普通表单字符串传入）：

```bash
curl -s \
  -F "node_list=@samples/feasible/nodes.json" \
  -F "pod_list=@samples/feasible/pods.json" \
  -F "drained_nodes=<samples/feasible/drained-nodes.json" \
  http://localhost:8000/api/plan
```

## 示例快照

重新生成：`python3 scripts/generate_samples.py samples`

| 场景 | 目录 | 预期结果 |
| --- | --- | --- |
| 可迁移 | `samples/feasible/` | 4 个待迁 Pod 全部落地；两个 `app=web` Pod 因硬反亲和分到不同节点；`worker-1` 受 nodeSelector 限制不去 batch 节点；DaemonSet Pod 列入排除项；Succeeded Pod 不占资源 |
| 资源超配 | `samples/infeasible-resource/` | 大 Pod 在两个承接节点上 CPU/内存均不足，无完整方案并给出逐节点缺口 |
| 反亲和无解 | `samples/infeasible-anti/` | 两个互相反亲和的 web Pod 只剩一个合格承接节点（另一节点已有 web、第三个节点不 Ready），回溯失败 |
| 不支持约束 | `samples/unsupported/` | 多容器+卷、init 容器 Pod 被拒绝；勾选污点节点同样被拒绝 |

## 自测

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

## 已知边界（未实现/刻意不支持）

- 不支持硬 `nodeAffinity`、硬 `podAffinity`、非 hostname 拓扑键与 `matchExpressions`；遇到即报错并撤下方案
- 不处理 PVC/存储卷、init 容器、多容器（sidecar）Pod
- 软亲和/反亲和（`preferredDuringScheduling*`）不参与硬性可调度性判断，直接忽略
- 不模拟真实调度器的 taints/tolerations 联动、端口占用、设备插件等；无污点节点之外的节点不参与
- 只做静态 requests 核算，不考虑超售比例与实时指标
