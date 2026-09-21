# 事故因果时间线工作台

本地运行、零外部依赖的事故调查工作台：接收应用 / 数据库 / 部署系统的 JSON 事件，
把**原始事实只追加保存**，由人工把事件归入事故、声明先后/因果边，并在
原始接收顺序、原始事件时间、校正后时间三种视图间切换。

只依赖 Go 标准库；不需要 Docker、云账号或独立数据库，数据只落在项目目录内。

## 运行

```sh
go test ./... -count=1
go run ./cmd/server --addr 127.0.0.1:5210
# 浏览器打开 http://127.0.0.1:5210
```

可选参数：`--data ./data`（追加式审计流目录，默认 `./data`）。

## 持久化模型

所有状态变更都先序列化为一条 JSON 记录、追加写入 `data/audit.log` 并 `fsync`，
然后才更新内存索引。启动时重放审计流还原全部状态：

- 进程被 kill 后重启，事故、事件、接收历史、时间版本、边（含逻辑删除标记）完全恢复。
- 写到一半的尾部记录（崩溃残留）会在启动时被安全截断，不影响前面的有效记录。
- 原始事件 JSON 在首次接收后永不改变；时间校正只追加新 `version`，旧版本可查。
- 事件删除与边删除都是**逻辑撤销**（撤销标记本身也是一条审计记录）。

## 关键规则

- **去重**：以 `(source, event_id)` 为唯一键。重送不覆盖 `raw`，只累加
  `receive_count`、`last_seen_at` 与一条接收记录（receipt）。
- **稳定排序**：按时间排序时，时间戳相等以全局接收序号 `seq` 破并列；
  迟到事件按其声明时间归位，接收序号不变。
- **时间校正**：必须携带所基于的 `expected_version`；过期版本返回 `409`，
  服务端互斥保证并发校正只有一个赢家。
- **成环拒绝**：新边会在活动边构成的 DAG 上做可达性检查；若成环返回 `409`
  和一条可读路径，如 `app/A (evt_…) -> app/B -> app/C -> app/A`。
- **撤销保护**：事件仍被任何活动边引用时拒绝撤销（`409`），先删边才能撤销。
- **部分批量失败**：批量导入逐行处理，单行失败不回滚其他行，响应逐行给出
  `line / ok / duplicate / error`。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/api/health` | 健康检查 |
| GET  | `/api/incidents` | 事故列表（含事件/边计数） |
| POST | `/api/incidents` | `{"title":"..."}` 新建事故 |
| GET  | `/api/incidents/{id}?sort=reception\|original\|corrected` | 事故详情与事件/边 |
| POST | `/api/incidents/{id}/events` | 单条投递 |
| POST | `/api/incidents/{id}/events/batch` | 批量投递，逐行返回结果 |
| GET  | `/api/events/{id}` | 事件详情：原始 JSON、版本、接收历史 |
| POST | `/api/events/{id}/correct` | `{"occurred_at","note","expected_version"}` |
| POST | `/api/events/{id}/revoke` | 逻辑撤销 |
| POST | `/api/incidents/{id}/edges` | `{"cause_id","effect_id","rationale"}` |
| DELETE | `/api/edges/{edgeID}` | 逻辑删除边 |

单条投递请求体：

```json
{
  "source": "pay-gateway",
  "event_id": "evt-001",
  "reason": "retry after timeout",
  "payload": { "occurred_at": "2026-09-21T10:02:00Z", "level": "error" }
}
```

`payload` 必须是 JSON 对象且包含 RFC3339 的 `occurred_at`；该字段作为 v1 时间版本。
首次投递返回 `201`，识别为重复投递返回 `200` 且 `duplicate=true`。

## 浏览器界面

- 左栏新建/选择事故；中栏在「原始接收顺序 / 原始时间 / 校正后时间」间切换。
- 右上角「批量导入事件」粘贴 JSON 数组，弹窗逐行显示入库 / 重复 / 失败原因。
- 因果连边两种方式：把一张事件卡**拖到**另一张，或点「选择两个事件连边」
  按“原因 → 结果”点选两张卡；提交时必须填写边成立的理由。
- 点击事件卡打开详情：原始事实、全部时间版本、接收历史；可提交校正或逻辑撤销。

## 测试

`go test ./... -count=1`（另建议 `go test -race ./...`）覆盖：
重复投递不可改写、迟到排序与同时间戳稳定破并列、成环拒绝与可读路径、
部分批量失败、并发版本冲突、引用阻止撤销、崩溃（含半行残留）后恢复。
