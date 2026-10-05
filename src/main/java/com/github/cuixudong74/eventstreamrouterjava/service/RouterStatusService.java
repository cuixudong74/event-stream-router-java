package com.github.cuixudong74.eventstreamrouterjava.service;

import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import com.github.cuixudong74.eventstreamrouterjava.dispatch.CheckpointStore;
import com.github.cuixudong74.eventstreamrouterjava.dispatch.DownstreamManager;
import com.github.cuixudong74.eventstreamrouterjava.journal.EventJournal;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingTable;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaRegistry;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 运行状态快照：已提交位点、各下游检查点与滞后、规则版本、支持的结构版本。
 * 已提交（journal lastSeq）与未完成投递（checkpoint 落后部分）在此可区分。
 */
@Service
public class RouterStatusService {

    private final EventJournal journal;
    private final CheckpointStore checkpoints;
    private final DownstreamManager downstreamManager;
    private final RoutingTable routingTable;
    private final SchemaRegistry schemaRegistry;
    private final RouterProperties properties;

    public RouterStatusService(EventJournal journal, CheckpointStore checkpoints,
                               DownstreamManager downstreamManager, RoutingTable routingTable,
                               SchemaRegistry schemaRegistry, RouterProperties properties) {
        this.journal = journal;
        this.checkpoints = checkpoints;
        this.downstreamManager = downstreamManager;
        this.routingTable = routingTable;
        this.schemaRegistry = schemaRegistry;
        this.properties = properties;
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("committedSeq", journal.lastSeq());
        status.put("targetSchemaVersion", properties.getTargetSchemaVersion());
        status.put("supportedSchemaVersions", schemaRegistry.supportedVersions());
        status.put("maxLag", properties.getMaxLag());
        status.put("overflowPolicy", properties.getOverflowPolicy().name());
        status.put("checkpoints", checkpoints.all());
        status.put("lag", downstreamManager.lagSnapshot());
        status.put("downstreams", downstreamManager.list());
        status.put("ruleSetVersions", routingTable.versions().stream()
                .map(v -> Map.of("version", v.version(), "effectiveFromSeq", v.effectiveFromSeq(),
                        "ruleCount", v.rules().size()))
                .toList());
        return status;
    }
}
