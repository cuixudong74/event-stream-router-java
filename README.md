# Event Stream Router

事件流路由服务接收业务事件，把不同结构版本的事件校验、转换到统一目标版本，并按在位点生效的规则版本投递到相互独立的下游。

## 语义总览

- 支持事件版本 `1` 和 `2`。
- 下游统一消费版本 `2`。
- 事件按全局单调递增的 `sequence` 落盘。
- 路由决策在入口处固化，包含 `sequence`、规则版本、规则 ID 和目标下游。
- 投递语义是 **at-least-once**：允许故障或重放造成重复，下游必须使用事件标识，必要时结合 `sequence` 做幂等去重。
- 同一下游只有一个投递工作线程，按该下游队列顺序投递；同一分区键因此保持提交顺序。
- 每个下游有独立有界队列、独立线程和独立提交位点，一个下游变慢或失败不会阻塞其他下游。

## 事件接入与版本校验

`POST /api/events`

```json
{
  "eventId": "evt-001",
  "eventType": "order.created",
  "version": "1",
  "partitionKey": "customer-1",
  "payload": {
    "amount": 100,
    "currency": "USD",
    "customerType": "RETAIL"
  }
}
```

校验失败返回 `422 UNPROCESSABLE_ENTITY`，错误数组定位到字段并给出错误码，例如：

- `REQUIRED`
- `TYPE`
- `RANGE`
- `FORMAT`
- `ENUM`
- `UNKNOWN_FIELD`
- `UNSUPPORTED_VERSION`

校验是全量校验，会尽量返回所有失败字段。服务拒绝未知版本和不兼容结构，不静默丢弃，也不会为缺失字段填默认值。

## 版本转换

当前内置规则：

### v1 → v2

| v1 字段 | v2 字段 | 说明 |
| --- | --- | --- |
| `payload.amount` | `payload.amount.value` | 数值原样复制 |
| `payload.currency` | `payload.amount.currency` | ISO 三字母货币码原样复制 |
| `payload.customerType` | `payload.customerType` | 原样复制 |
| `payload.occurredAt` | `payload.occurredAt` | 存在时原样复制 |
| 缺失 | `payload.occurredAt` | 不补当前时间，返回不可映射问题 |

转换状态：

- `CONVERTED`：所有目标字段均可确定性得到。
- `PARTIAL`：事件可以路由，但存在目标版本要求而源版本无法提供的字段，问题记录在 `conversionIssues`。
- `REJECTED`：预留给无法继续转换的未来规则；当前 v1/v2 规则只使用前两种状态。

转换规则以源版本到目标版本注册，重复注册不同实现会失败。转换结果与事件一起持久化，之后重放使用当时已固化的目标事件和问题，不依赖当前转换器代码。

## 路由规则版本

规则是不可变版本：

```json
{
  "ruleVersion": "rules-v2",
  "startsAtSequence": 100,
  "routes": [
    {
      "routeId": "business-to-payments",
      "eventType": "order.created",
      "payloadEquals": { "customerType": "BUSINESS" },
      "destinationId": "payments"
    }
  ],
  "defaultDestinationId": "orders"
}
```

- `startsAtSequence` 为包含位点。
- 规则按声明顺序做首个匹配。
- 无显式匹配时使用默认下游；没有默认下游则拒绝该事件。
- 位点 `n` 只查询 `startsAtSequence <= n` 的最高规则版本。
- 每个事件在入口只计算一次，决策与事件一起持久化。
- 已持久化事件重放不会因规则后来变化而改投。

通过 `POST /api/rules` 发布未来起始位点的新规则，通过 `GET /api/rules` 查看规则历史。

## 下游管理、保序与去重

新增下游：

`POST /api/downstreams`

```json
{
  "id": "payments",
  "startMode": "BEGINNING",
  "queueCapacity": 1024,
  "policy": "BLOCK_PRODUCER"
}
```

起点必须明确：

- `BEGINNING`：从位点 `1` 开始。
- `END`：从当前下一位点开始，只接收之后的事件。
- 数字字符串：从指定位点开始。

显式重放：

`POST /api/downstreams/{id}/replay?fromSequence=1`

重放会重建该下游自己的待投队列，不影响其他下游位点。为了明确暴露重复，它会把该下游的消费游标重置到指定位置；下游必须依赖 `eventId + sequence` 去重。

删除下游：

`DELETE /api/downstreams/{id}`

删除先进入 `DRAINING`，等待当前队列完全排空后再标记 `REMOVED`。为避免强制删除造成在途事件丢失，如果某个处理器永久失败，该下游会停留在可观测的 `DRAINING` 状态并继续暴露积压，直到处理器恢复或管理员通过显式重放/死信流程处理。若规则仍指向被删除的目标，后续事件会因没有活跃目标而被明确拒绝，不会静默丢失。生产上应先发布新规则，再删除旧下游。

替换接收处理器（重新注册同 ID 的实现）不会改变持久化进度，因为进度属于下游 ID，不属于处理器实例。

## 背压与可观测性

查询指标：

`GET /api/metrics`

下游指标包含：

- `committedSequence`：已被处理器成功接收并提交的位点。
- `inFlightSequence`：已投递但尚未提交的位点。
- `queueSize`：已持久化且待投递的事件数。
- `lagCount`：未提交且不在死信队列的事件数。
- `deliveredCount` / `deadLetterCount`。

有界容量把“在途投递 + 待投事件”共同计入，因此无处理器无限循环或内存无界堆积。策略包括：

- `BLOCK_PRODUCER`：容量满时阻塞提交，向生产者传导背压。
- `REJECT_PRODUCER`：容量满时返回 `503`，事件不分配结果、不落事件日志，可由生产者重试。
- `DEAD_LETTER`：容量满时事件仍落事件日志，并追加带原因的死信记录，结果可查询和后续处理。

慢或失败的下游只重试自己的下一个事件，不占用其他下游线程。

## 恢复与一致性边界

本地文件状态默认写入：

- `${router.data-directory:./data/router}/events.log`
- `state.json`
- `dead-letters.log`
- `rules.log`

提交位点只在下游处理器成功返回后更新。若在处理器执行期间重启：

1. 事件日志已经包含该事件。
2. 下游位点尚未提交。
3. 重启后该事件从未完成队列恢复并重新投递。
4. 下游通过幂等逻辑去掉允许的重复。

因此系统保证：

- 已持久化事件不会被跳过。
- 已提交位点不会被重置。
- 在途状态与已提交状态可区分。
- 路由和转换结果可重放复核。

当前实现使用本地 JSONL/快照存储，适合单进程服务和测试；多副本部署应把事件日志、位点和规则历史替换为外部线性一致存储，同时保留本项目的先持久化后可见、按下游串行投递、提交后确认等边界。
