package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.core.JournalEntry;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingRule;
import com.github.cuixudong74.eventstreamrouterjava.schema.EventSchema;
import com.github.cuixudong74.eventstreamrouterjava.schema.FieldType;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 规则切换边界：位点两侧的事件各自按其生效时的规则版本路由，任一事件只被一个版本命中一次。
 */
class RuleSwitchBoundaryTest {

    @TempDir
    Path dir;

    @Test
    void eventsOnBothSidesOfSwitchUseTheirOwnRuleVersion() {
        try (TestHarness h = new TestHarness(dir)) {
            h.schemas.register(new EventSchema(1, List.of(
                    new SchemaField("orderId", FieldType.STRING, true)), true));

            TestHarness.RecordingSink sinkA = new TestHarness.RecordingSink();
            TestHarness.RecordingSink sinkB = new TestHarness.RecordingSink();
            h.downstreams.registerSink("sinkA", sinkA);
            h.downstreams.registerSink("sinkB", sinkB);
            h.downstreams.addDownstream("A", "sinkA", Optional.empty());
            h.downstreams.addDownstream("B", "sinkB", Optional.empty());

            // 规则 v1：order -> A
            h.routing.activate(List.of(new RoutingRule("r1", "order", null, List.of("A"), 1)),
                    h.journal.lastSeq() + 1);

            long s1 = h.submit("e1", 1, "k1", "order", Map.of("orderId", "o1"));
            long s2 = h.submit("e2", 1, "k1", "order", Map.of("orderId", "o2"));

            // 规则 v2：order -> B，从下一个位点生效
            h.routing.activate(List.of(new RoutingRule("r1", "order", null, List.of("B"), 1)),
                    h.journal.lastSeq() + 1);

            long s3 = h.submit("e3", 1, "k1", "order", Map.of("orderId", "o3"));
            long s4 = h.submit("e4", 1, "k1", "order", Map.of("orderId", "o4"));

            List<JournalEntry> entries = h.journal.readFrom(1, 100);
            assertThat(entries).hasSize(4);
            // 位点两侧各自版本
            assertThat(entries.get(0).ruleSetVersion()).isEqualTo(1);
            assertThat(entries.get(1).ruleSetVersion()).isEqualTo(1);
            assertThat(entries.get(2).ruleSetVersion()).isEqualTo(2);
            assertThat(entries.get(3).ruleSetVersion()).isEqualTo(2);
            // 每个事件只命中一次、只进一个下游
            assertThat(entries.get(0).matchedDownstreams()).containsExactly("A");
            assertThat(entries.get(2).matchedDownstreams()).containsExactly("B");

            await().atMost(5, TimeUnit.SECONDS).until(() -> sinkA.eventIds().equals(List.of("e1", "e2")));
            await().atMost(5, TimeUnit.SECONDS).until(() -> sinkB.eventIds().equals(List.of("e3", "e4")));
            assertThat(s2).isEqualTo(s1 + 1);
            assertThat(s4).isEqualTo(s3 + 1);
        }
    }

    @Test
    void unmatchedEventIsJournaledWithExplainableDropReason() {
        try (TestHarness h = new TestHarness(dir)) {
            h.schemas.register(new EventSchema(1, List.of(
                    new SchemaField("orderId", FieldType.STRING, true)), true));
            h.routing.activate(List.of(new RoutingRule("r1", "order", null, List.of("A"), 1)),
                    h.journal.lastSeq() + 1);

            var result = h.ingest.ingest(new com.github.cuixudong74.eventstreamrouterjava.core.EventEnvelope(
                    "e9", 1, "k1", "payment", Map.of("orderId", "o9"), 0));
            assertThat(result.matchedDownstreams()).isEmpty();
            assertThat(result.dropReason()).contains("no rule matched");
            // 未命中也提交日志，可观测、可解释，不是静默丢弃
            assertThat(h.journal.readFrom(result.seq(), 1).get(0).dropReason()).isNotNull();
        }
    }
}
