package com.github.cuixudong74.eventstreamrouterjava.api;

import com.github.cuixudong74.eventstreamrouterjava.dispatch.Downstream;
import com.github.cuixudong74.eventstreamrouterjava.dispatch.DownstreamManager;
import com.github.cuixudong74.eventstreamrouterjava.journal.EventJournal;
import com.github.cuixudong74.eventstreamrouterjava.routing.RuleSetVersion;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingRule;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingTable;
import com.github.cuixudong74.eventstreamrouterjava.schema.EventSchema;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 管理端点：结构版本注册、规则版本激活、下游动态增删与重放。
 * 规则激活与位点分配在同一临界区，保证生效位点明确、两侧事件各按其版本路由。
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final SchemaRegistry schemaRegistry;
    private final RoutingTable routingTable;
    private final DownstreamManager downstreamManager;
    private final EventJournal journal;

    public AdminController(SchemaRegistry schemaRegistry, RoutingTable routingTable,
                           DownstreamManager downstreamManager, EventJournal journal) {
        this.schemaRegistry = schemaRegistry;
        this.routingTable = routingTable;
        this.downstreamManager = downstreamManager;
        this.journal = journal;
    }

    @PostMapping("/schemas")
    public ResponseEntity<Map<String, Object>> registerSchema(@RequestBody EventSchema schema) {
        schemaRegistry.register(schema);
        return ResponseEntity.ok(Map.of("registered", schema.version(),
                "supported", schemaRegistry.supportedVersions()));
    }

    @PostMapping("/rulesets")
    public ResponseEntity<Map<String, Object>> activateRules(@RequestBody List<RoutingRule> rules) {
        RuleSetVersion version;
        // 与 IngestService 的位点分配共用同一临界区，生效位点 = 下一个待分配位点。
        synchronized (journal) {
            version = routingTable.activate(rules, journal.lastSeq() + 1);
        }
        return ResponseEntity.ok(Map.of("version", version.version(),
                "effectiveFromSeq", version.effectiveFromSeq()));
    }

    @PostMapping("/downstreams")
    public ResponseEntity<Map<String, Object>> addDownstream(@RequestBody Map<String, Object> request) {
        String id = (String) request.get("id");
        String sinkName = (String) request.get("sink");
        Optional<Long> fromSeq = Optional.ofNullable(request.get("fromSeq"))
                .map(v -> ((Number) v).longValue());
        Downstream d = downstreamManager.addDownstream(id, sinkName, fromSeq);
        return ResponseEntity.ok(Map.of("id", d.id(), "startSeq", d.startSeq()));
    }

    @DeleteMapping("/downstreams/{id}")
    public ResponseEntity<Map<String, Object>> removeDownstream(@PathVariable String id) {
        boolean removed = downstreamManager.removeDownstream(id);
        return ResponseEntity.ok(Map.of("id", id, "removed", removed));
    }

    @PostMapping("/downstreams/{id}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable String id,
                                                      @RequestBody Map<String, Object> request) {
        long fromSeq = ((Number) request.get("fromSeq")).longValue();
        downstreamManager.replay(id, fromSeq);
        return ResponseEntity.ok(Map.of("id", id, "replayFromSeq", fromSeq));
    }
}
