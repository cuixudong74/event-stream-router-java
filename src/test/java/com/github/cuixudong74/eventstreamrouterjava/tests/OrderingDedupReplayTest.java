package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.core.Delivery;
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
 * 分区保序、按事件标识去重、指定位点重放一致性。
 */
class OrderingDedupReplayTest {

    @TempDir
    Path dir;

    private TestHarness harnessWithAllToA(TestHarness.RecordingSink sink) {
        TestHarness h = new TestHarness(dir);
        h.schemas.register(new EventSchema(1, List.of(
                new SchemaField("orderId", FieldType.STRING, true)), true));
        h.downstreams.registerSink("sinkA", sink);
        h.downstreams.addDownstream("A", "sinkA", Optional.empty());
        h.routing.activate(List.of(new RoutingRule("all", null, null, List.of("A"), 1)),
                h.journal.lastSeq() + 1);
        return h;
    }

    @Test
    void perPartitionOrderIsPreserved() {
        TestHarness.RecordingSink sink = new TestHarness.RecordingSink();
        try (TestHarness h = harnessWithAllToA(sink)) {
            // 交错提交两个分区键
            h.submit("e1", 1, "k1", "order", Map.of("orderId", "1"));
            h.submit("e2", 1, "k2", "order", Map.of("orderId", "2"));
            h.submit("e3", 1, "k1", "order", Map.of("orderId", "3"));
            h.submit("e4", 1, "k2", "order", Map.of("orderId", "4"));
            h.submit("e5", 1, "k1", "order", Map.of("orderId", "5"));

            await().atMost(5, TimeUnit.SECONDS).until(() -> sink.deliveries.size() == 5);

            List<String> k1 = sink.deliveries.stream()
                    .filter(d -> d.partitionKey().equals("k1")).map(Delivery::eventId).toList();
            List<String> k2 = sink.deliveries.stream()
                    .filter(d -> d.partitionKey().equals("k2")).map(Delivery::eventId).toList();
            assertThat(k1).containsExactly("e1", "e3", "e5");
            assertThat(k2).containsExactly("e2", "e4");
        }
    }

    @Test
    void duplicatesAreIdentifiableByEventIdAndSeq() {
        // 模拟“投递成功但检查点未推进就崩溃”的 sink：第一次记录后抛错，触发重投
        TestHarness.RecordingSink sink = new TestHarness.RecordingSink() {
            private boolean crashed;
            @Override
            public void deliver(Delivery delivery) throws Exception {
                if (!crashed && delivery.eventId().equals("e2")) {
                    crashed = true;
                    deliveries.add(delivery); // 已送达
                    throw new RuntimeException("crash after delivery before checkpoint");
                }
                super.deliver(delivery);
            }
        };
        try (TestHarness h = harnessWithAllToA(sink)) {
            h.submit("e1", 1, "k1", "order", Map.of("orderId", "1"));
            h.submit("e2", 1, "k1", "order", Map.of("orderId", "2"));
            h.submit("e3", 1, "k1", "order", Map.of("orderId", "3"));

            await().atMost(5, TimeUnit.SECONDS).until(() -> sink.deliveries.size() >= 4);

            // e2 出现两次，但 (eventId, seq) 相同，下游可据此去重
            List<Delivery> dups = sink.deliveries.stream().filter(d -> d.eventId().equals("e2")).toList();
            assertThat(dups).hasSize(2);
            assertThat(dups.get(0).seq()).isEqualTo(dups.get(1).seq());
            // 去重后的有效顺序仍保持提交顺序
            List<String> deduped = sink.deliveries.stream()
                    .map(d -> d.eventId() + "@" + d.seq()).distinct().toList();
            assertThat(deduped).containsExactly("e1@1", "e2@2", "e3@3");
        }
    }

    @Test
    void replayFromPositionMatchesFirstDelivery() {
        TestHarness.RecordingSink sink = new TestHarness.RecordingSink();
        try (TestHarness h = harnessWithAllToA(sink)) {
            for (int i = 1; i <= 5; i++) {
                h.submit("e" + i, 1, "k1", "order", Map.of("orderId", String.valueOf(i)));
            }
            await().atMost(5, TimeUnit.SECONDS).until(() -> sink.deliveries.size() == 5);
            List<String> firstRun = sink.eventIds();

            // 从位点 2 重放
            sink.deliveries.clear();
            h.downstreams.replay("A", 2);
            await().atMost(5, TimeUnit.SECONDS).until(() -> sink.deliveries.size() == 4);

            assertThat(sink.eventIds()).containsExactlyElementsOf(firstRun.subList(1, 5));
        }
    }
}
