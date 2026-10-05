package com.github.cuixudong74.eventstreamrouterjava.service;

import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import com.github.cuixudong74.eventstreamrouterjava.convert.EventConverter;
import com.github.cuixudong74.eventstreamrouterjava.core.ConversionOutcome;
import com.github.cuixudong74.eventstreamrouterjava.core.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.core.JournalEntry;
import com.github.cuixudong74.eventstreamrouterjava.core.ValidationResult;
import com.github.cuixudong74.eventstreamrouterjava.dispatch.DownstreamManager;
import com.github.cuixudong74.eventstreamrouterjava.journal.EventJournal;
import com.github.cuixudong74.eventstreamrouterjava.routing.RuleSetVersion;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingRule;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingTable;
import com.github.cuixudong74.eventstreamrouterjava.schema.EventSchema;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaRegistry;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaValidator;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 接入流水线：背压检查 -> 结构校验 -> 版本转换 -> 规则路由 -> 提交日志。
 * 任一环节失败即拒绝并给出可定位原因，不静默丢弃、不补默认值放行。
 * 位点分配、规则版本定位与日志落盘在同一临界区完成，
 * 保证规则切换位点两侧的事件各自按其生效时的规则版本路由。
 */
@Service
public class IngestService {

    private final SchemaRegistry schemaRegistry;
    private final SchemaValidator validator;
    private final EventConverter converter;
    private final RoutingTable routingTable;
    private final EventJournal journal;
    private final DownstreamManager downstreamManager;
    private final RouterProperties properties;

    public IngestService(SchemaRegistry schemaRegistry, SchemaValidator validator, EventConverter converter,
                         RoutingTable routingTable, EventJournal journal,
                         DownstreamManager downstreamManager, RouterProperties properties) {
        this.schemaRegistry = schemaRegistry;
        this.validator = validator;
        this.converter = converter;
        this.routingTable = routingTable;
        this.journal = journal;
        this.downstreamManager = downstreamManager;
        this.properties = properties;
    }

    public IngestResult ingest(EventEnvelope envelope) {
        checkBackpressure();
        validateEnvelope(envelope);

        EventSchema schema = schemaRegistry.find(envelope.schemaVersion())
                .orElseThrow(() -> new IngestException(422,
                        "unsupported schemaVersion " + envelope.schemaVersion()
                                + "; supported: " + schemaRegistry.supportedVersions()));
        ValidationResult validation = validator.validate(schema, envelope.payload());
        if (!validation.valid()) {
            throw new IngestException(422, "schema validation failed: " + validation.errors());
        }

        ConversionOutcome outcome = converter.convert(
                envelope.payload() == null ? Map.of() : envelope.payload(),
                envelope.schemaVersion(), properties.getTargetSchemaVersion());
        if (outcome.status() == ConversionOutcome.Status.FAILED) {
            throw new IngestException(422, "conversion to target version "
                    + properties.getTargetSchemaVersion() + " failed: " + outcome.issues());
        }

        // 临界区：位点分配 + 规则版本定位 + 落盘必须原子，保证位点-规则版本对应关系确定。
        synchronized (journal) {
            long seq = journal.lastSeq() + 1;
            RuleSetVersion ruleSet = routingTable.versionFor(seq).orElse(null);
            List<String> matched = new ArrayList<>();
            String dropReason = null;
            if (ruleSet == null) {
                dropReason = "no active ruleset for seq " + seq;
            } else {
                for (RoutingRule rule : ruleSet.rules()) {
                    if (rule.matches(envelope.eventType(), envelope.partitionKey())) {
                        matched.addAll(rule.downstreamIds());
                        break; // 先中先得，一个事件只命中一次
                    }
                }
                if (matched.isEmpty()) {
                    dropReason = "no rule matched in ruleset v" + ruleSet.version();
                }
            }
            JournalEntry entry = new JournalEntry(seq, envelope.eventId(), envelope.partitionKey(),
                    envelope.eventType(), envelope.schemaVersion(), properties.getTargetSchemaVersion(),
                    outcome.payload(), outcome.issues(),
                    ruleSet == null ? 0 : ruleSet.version(), List.copyOf(matched), dropReason,
                    System.currentTimeMillis());
            journal.append(entry);
            return new IngestResult(seq, entry.ruleSetVersion(), entry.matchedDownstreams(),
                    entry.conversionIssues(), entry.dropReason());
        }
    }

    private void checkBackpressure() {
        if (properties.getOverflowPolicy() != RouterProperties.OverflowPolicy.REJECT_INGEST) {
            return;
        }
        for (Map.Entry<String, Long> lag : downstreamManager.lagSnapshot().entrySet()) {
            if (lag.getValue() > properties.getMaxLag()) {
                throw new IngestException(429, "backpressure: downstream '" + lag.getKey()
                        + "' lag " + lag.getValue() + " exceeds maxLag " + properties.getMaxLag()
                        + "; policy=REJECT_INGEST, retry later");
            }
        }
    }

    private void validateEnvelope(EventEnvelope envelope) {
        if (envelope == null) {
            throw new IngestException(400, "request body is required");
        }
        if (envelope.eventId() == null || envelope.eventId().isBlank()) {
            throw new IngestException(400, "eventId is required (used for downstream dedup)");
        }
        if (envelope.partitionKey() == null || envelope.partitionKey().isBlank()) {
            throw new IngestException(400, "partitionKey is required (used for ordered delivery)");
        }
        if (envelope.eventType() == null || envelope.eventType().isBlank()) {
            throw new IngestException(400, "eventType is required (used for routing)");
        }
    }
}
