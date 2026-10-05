# event-stream-router-java

事件流路由服务：上游持续提交业务事件，服务按版本化规则把事件分流到多个相互独立的下游消费方，
各方按自己的进度消费。事件结构与路由规则均可演进，演进过程中下游不会看到丢失、重复（不可去重的）
或错位的数据。

## 运行

```bash
mvn spring-boot:run          # 默认端口见 application.properties
mvn test                     # 全部自动化测试
```

配置项（`router.*`）：

| 配置 | 默认 | 说明 |
|---|---|---|
| `router.data-dir` | `data` | 持久化目录（日志、检查点、注册表） |
| `router.target-schema-version` | `1` | 下游统一消费的目标结构版本 |
| `router.max-lag` | `10000` | 单下游允许的最大滞后位点数 |
| `router.overflow-policy` | `REJECT_INGEST` | 超上界策略：拒绝新提交并说明原因 |
| `router.retry-backoff-millis` | `200` | 投递失败重试间隔 |

## 核心概念

### 位点（sequence）

每个通过校验的事件在提交时被分配一个单调递增的位点 `seq`，并以 JSONL 形式追加到
`journal.log`（写即落盘）。日志是系统唯一事实来源：路由决策、转换结果在写入时确定并随记录
持久化，因此**从任意位点重放的结果与首次投递完全一致**。

### 结构版本校验

- 结构按版本注册（`POST /admin/schemas`），版本一经注册不可变。
- 生产者可用任一受支持版本提交（`POST /events`，携带 `schemaVersion`）。
- 校验失败（未知版本、缺必填字段、类型不匹配、未声明字段）一律拒绝并返回 `422`，
  错误信息定位到具体字段（`field` + `code` + `message`）。
  **不静默丢弃，也不按缺省值补全后放行。**

### 版本转换

- 转换规则按相邻版本维护（`VersionConversion`，持久化于 `conversions.json`），
  支持 `COPY / RENAME / INT_TO_STRING / STRING_TO_INT / CONSTANT / DROP`。
- 接入时把事件确定性地转换到 `target-schema-version`：同一事件无论何时到达，转换结果一致
  （规则持久化、无时间/随机因素）。
- 无法转换的字段产生可区分的 `ConversionIssue`（`MISSING_SOURCE / INCOMPATIBLE_VALUE /
  NO_RULE`），结果状态为 `CONVERTED / PARTIAL / FAILED`；`FAILED` 整体拒绝（422），
  `PARTIAL` 随日志记录并可在接入响应与日志中观察到。

### 路由语义

- 规则集版本化（`POST /admin/rulesets`），每个版本在明确位点 `effectiveFromSeq`（含）生效。
- 位点分配、规则版本定位、日志落盘在同一临界区完成，因此**位点两侧的事件各自按其生效时的
  规则版本路由**，不存在跨版本混用。
- 一个事件只被一个规则版本评估一次；版本内规则按 `priority` 排序、先中先得，
  命中后投递到该规则的 `downstreamIds`。
- 未命中任何规则的事件也会提交日志，携带 `dropReason`（可观测、可解释），不静默丢弃。

### 投递语义

- 每个下游一个独立投递线程，按 `seq` 顺序扫描日志并过滤命中自己的记录：
  **同一分区键的事件在下游保持提交顺序**。
- 投递为至少一次：失败按固定间隔重试；崩溃恢复后从检查点重投，可能产生重复。
  每条 `Delivery` 携带 `eventId` 与 `seq`，**下游按事件标识去重**。
- 下游通过 `DownstreamSink` 接口接入（`registerSink` 注册名字，下游按名字绑定）。

### 隔离与背压

- 下游之间完全隔离：慢下游只增加自身滞后，不阻塞其他下游与接入端。
- 滞后 = 日志最新位点 − 该下游已确认位点，经 `GET /status` 可观测（含各下游检查点）。
- 任一下游滞后超过 `max-lag` 时，按 `REJECT_INGEST` 策略拒绝新提交（`429`），
  错误信息指明是哪个下游、当前滞后量与策略，接入端不会无界堆积。

### 动态变更

- `POST /admin/downstreams` 新增下游：从明确起点开始（默认当前日志头部，可显式 `fromSeq`）。
- `DELETE /admin/downstreams/{id}` 删除下游：停止其投递循环，保留检查点，不影响其他下游。
- 规则变更见“路由语义”：在途事件按各自位点的规则版本路由，不丢失、不错位；
  已有下游的进度不因任何变更被重置。

### 恢复

- 重启后从 `journal.log` 恢复已提交位点，从 `checkpoints.json` 恢复各下游进度，
  从 `downstreams.json` 恢复下游注册表并自动重启投递循环。
- 已提交（`committedSeq`）与未完成投递（检查点落后部分）在 `/status` 中可区分。
- 检查点只在投递成功后推进；停止发生在投递中途时不推进检查点，
  因此**恢复不会跳过事件**，只会产生可被去重的重复。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/events` | 提交事件；202 返回 `{seq, ruleSetVersion, matchedDownstreams, conversionIssues, dropReason}`，4xx 返回结构化原因 |
| POST | `/admin/schemas` | 注册结构版本 |
| POST | `/admin/rulesets` | 激活规则集新版本（生效位点 = 下一个待分配位点） |
| POST | `/admin/downstreams` | 新增下游 `{id, sink, fromSeq?}` |
| DELETE | `/admin/downstreams/{id}` | 删除下游 |
| POST | `/admin/downstreams/{id}/replay` | 从指定位点重放 `{fromSeq}` |
| GET | `/status` | 已提交位点、检查点、滞后、规则版本、支持的结构版本 |

## 一致性边界

- **单进程内**位点分配与规则激活互斥，位点-规则版本对应关系确定。
- 日志为单文件顺序追加，适合单实例部署；多实例横向扩展需要外部分布式日志，不在本仓库范围。
- 至少一次投递 + 下游按 `eventId` 去重 = 效果上等价于恰好一次；服务本身不承诺恰好一次投递。
- 未命中规则的事件不投递但留痕（`dropReason`），属于可解释丢弃而非静默丢失。

## 测试覆盖

`src/test/java/.../tests/`：

- `SchemaValidationTest` — 结构版本不兼容：逐字段定位、拒绝而非补默认值
- `ConversionTest` — 转换确定性、链式转换、无法转换字段的可区分结果
- `RuleSwitchBoundaryTest` — 规则切换位点两侧各按其版本路由、未命中留痕
- `OrderingDedupReplayTest` — 分区保序、重复可去重、重放与首次一致
- `BackpressureTest` — 慢下游隔离、滞后可观测、超上界按策略拒绝
- `DynamicAddRemoveTest` — 运行中增删下游与规则、进度不重置、新下游明确起点
- `RecoveryTest` — 重启恢复不跳过事件、重复可去重、位点连续
- `ApiEndToEndTest` — HTTP 端到端：422 结构化拒绝原因、202 位点返回、状态可观测
