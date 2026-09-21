# 事故因果时间线工作台（Incident Causal Timeline）

一个完全本地运行的单进程工作台：接收来自应用 / 数据库 / 部署系统的 JSON 事件，
把原始事件**逐字节**追加保存，再由值班人员把事件归入事故、声明先后约束
（`before`）与可能因果边（`causes`），并在浏览器里同时查看：

- **原始接收顺序**（按首次接收到该事件的审计序号）
- **按事件时间整理的时间线**（原始 / 校正后可切换）
- **每条边为什么成立**（类型 + 使用者填写的依据 + 审计序号）

无 Docker、无云账号、无独立数据库；只有 Go 标准库，所有数据落在项目目录内
（默认 `./data/audit.log`，一行一条 JSON 的追加式审计流）。

## 运行

```bash
go test ./... -count=1
go run ./cmd/server --addr 127.0.0.1:5210
# 打开 http://127.0.0.1:5210 ，页面标题为“因果时间线”
```

`--data` 可指定审计流目录（默认 `data`，已在 .gitignore 中忽略）。

## 关键语义

- **原始事实不可变**：首次见到的 `source + event_id` 的原始字节永久保存；
  时间戳被更正后，“原始时间线”视图仍使用原始时间戳。
- **重复投递**：同一 `source + event_id` 只保留一份事件，但每次投递都会
  追加一条接收记录，`receive_count` 累计，最近接收时间可在版本/接收历史中查看。
- **迟到事件**：时间线严格按事件时间排序；相同时间戳用首次接收序号决胜，
  顺序稳定、全序。
- **时间校正**：校正只追加新版本（0 为原始版本），旧版本永久可查；
  提交必须带 `base_version`，并发时旧版本号得到 `409 version_conflict`。
- **因果/先后边**：成环一律拒绝，返回 `409 cycle_detected` 和可读环路路径
  （如 `app/c -> app/a -> app/b -> app/c`）。
- **逻辑撤销**：`revoke` 只置撤销标记；任何事故中仍有有效边引用该事件时拒绝撤销。
- **批量导入**：逐行独立提交、独立校验；坏行不影响好行，响应逐行给出结果。
- **崩溃恢复**：每条记录写入后 `fsync`；启动时重放审计流。末尾残缺行
  （写入中途被杀）会被截断忽略，已确认提交的记录完整还原。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/incidents` | `{"title":...}` 新建事故 |
| `GET`  | `/api/incidents` | 事故列表 |
| `GET`  | `/api/incidents/{id}?corrected=0|1` | 接收顺序 + 时间线 + 边 |
| `POST` | `/api/incidents/{id}/ingest` | 单条原始事件 JSON（原文入库） |
| `POST` | `/api/incidents/{id}/bulk` | `{"events":[...],...}` 批量，逐行结果 |
| `POST` | `/api/incidents/{id}/members` | 把已存在事件加入事故 |
| `POST` | `/api/incidents/{id}/edges` | 声明 `{from,to,kind,rationale}` |
| `DELETE` | `/api/incidents/{id}/edges?from=&to=` | 逻辑删除边 |
| `POST` | `/api/events/ingest` `/api/events/bulk` | 不带事故的入库（之后再分配） |
| `GET`  | `/api/event?key=source/event_id` | 事件详情：原始报文、版本、接收历史 |
| `POST` | `/api/event/correct` | `{key,timestamp,note,base_version}` |
| `POST` | `/api/event/revoke` | 逻辑撤销 |

事件 key 为 `source/event_id`。事件 JSON 至少包含 `source`、`event_id`、
RFC3339 `timestamp`，其余任意字段原样保留。

## 代码结构

- `cmd/server` — 入口（`--addr`、`--data`）
- `internal/store` — 追加式审计流、重放恢复、事件/事故/边/版本模型与规则
- `internal/api` — HTTP API 与 `go:embed` 的零依赖浏览器界面（`web/`）
- `internal/store/*_test.go` — 重复投递、迟到与同戳排序、成环拒绝、
  部分批量失败、并发版本冲突、崩溃恢复、撤销保护、残行容忍等测试
