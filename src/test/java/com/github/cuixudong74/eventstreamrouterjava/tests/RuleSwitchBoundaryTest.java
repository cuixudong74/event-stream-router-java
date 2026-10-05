package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.model.RouteDefinition;
import com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion;
import com.github.cuixudong74.eventstreamrouterjava.service.RuleAdminService;
import com.github.cuixudong74.eventstreamrouterjava.tests.support.RouterTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RuleSwitchBoundaryTest {
    @TempDir
    Path directory;

    @Test
    void eventsOnBothSidesUseRuleVersionEffectiveAtSequence() throws Exception {
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            harness.downstreams.add("orders", "BEGINNING", 10,
                    com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy.BLOCK_PRODUCER);
            harness.downstreams.add("audit", "BEGINNING", 10,
                    com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("orders", RouterTestHarness.receiver("orders", List.of()));
            harness.register("audit", RouterTestHarness.receiver("audit", List.of()));

            var before = harness.router.submit(harness.objectMapper.readTree("""
                    {
                      "eventId":"e-before",
                      "eventType":"order.created",
                      "version":"2",
                      "partitionKey":"p-1",
                      "payload":{"amount":{"value":1,"currency":"USD"},"customerType":"RETAIL","occurredAt":"2026-01-01T00:00:00Z"}
                    }
                    """));
            assertEquals("rules-v1", before.ruleVersion());
            assertEquals("all-orders", harness.store.read(1).orElseThrow().decision().routeId());

            long startAt = harness.store.nextSequence();
            new RuleAdminService(harness.rules, harness.store).publish(new RuleVersion(
                    "rules-v2", startAt,
                    List.of(new RouteDefinition("all-to-audit", "order.created", Map.of(), "audit")),
                    "audit"));

            var after = harness.router.submit(harness.objectMapper.readTree("""
                    {
                      "eventId":"e-after",
                      "eventType":"order.created",
                      "version":"2",
                      "partitionKey":"p-1",
                      "payload":{"amount":{"value":2,"currency":"USD"},"customerType":"RETAIL","occurredAt":"2026-01-01T00:01:00Z"}
                    }
                    """));

            assertEquals("rules-v1", harness.store.read(1).orElseThrow().decision().ruleVersion());
            assertEquals("rules-v2", after.ruleVersion());
            assertEquals("audit", after.destinationId());
            assertEquals("all-to-audit", harness.store.read(2).orElseThrow().decision().routeId());
        }
    }
}
