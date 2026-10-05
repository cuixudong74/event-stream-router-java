package com.github.cuixudong74.eventstreamrouterjava.tests;

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
 * 运行中动态增删路由与下游：在途事件不丢失不错位，已有下游进度不重置，新下游从明确起点开始。
 */
class DynamicAddRemoveTest {

    @TempDir
    Path dir;

    @Test
    void addAndRemoveDownstreamWhileRunning() {
        try (TestHarness h = new TestHarness(dir)) {
            h.schemas.register(new EventSchema(1, List.of(
                    new SchemaField("orderId", FieldType.STRING, true)), true));
            TestHarness.RecordingSink sinkA = new TestHarness.RecordingSink();
            TestHarness.RecordingSink sinkB = new TestHarness.RecordingSink();
            h.downstreams.registerSink("sinkA", sinkA);
            h.downstreams.registerSink("sinkB", sinkB);
            h.downstreams.addDownstream("A", "sinkA", Optional.empty());
            h.routing.activate(List.of(new RoutingRule("r1", "order", null, List.of("A"), 1)),
                    h.journal.lastSeq() + 1);

            h.submit("e1", 1, "k1", "order", Map.of("orderId", "1"));
            h.submit("e2", 1, "k1", "order", Map.of("orderId", "2"));
            await().atMost(5, TimeUnit.SECONDS).until(() -> sinkA.deliveries.size() == 2);

            // 动态新增下游 B：从明确起点（当前日志头部）开始，收不到 e1/e2
            var added = h.downstreams.addDownstream("B", "sinkB", Optional.empty());
            assertThat(added.startSeq()).isEqualTo(3);
            // 路由升级到 v2：order -> A, B
            h.routing.activate(List.of(new RoutingRule("r1", "order", null, List.of("A", "B"), 1)),
                    h.journal.lastSeq() + 1);

            h.submit("e3", 1, "k1", "order", Map.of("orderId", "3"));
            await().atMost(5, TimeUnit.SECONDS).until(() -> sinkB.deliveries.size() == 1);
            await().atMost(5, TimeUnit.SECONDS).until(() -> sinkA.deliveries.size() == 3);

            // B 只收到起点之后的事件；A 的进度未被重置（无重复 e1/e2）
            assertThat(sinkB.eventIds()).containsExactly("e3");
            assertThat(sinkA.eventIds()).containsExactly("e1", "e2", "e3");

            // 动态删除 A：在途事件不丢失，B 继续
            assertThat(h.downstreams.removeDownstream("A")).isTrue();
            h.routing.activate(List.of(new RoutingRule("r1", "order", null, List.of("B"), 1)),
                    h.journal.lastSeq() + 1);
            h.submit("e4", 1, "k1", "order", Map.of("orderId", "4"));
            await().atMost(5, TimeUnit.SECONDS).until(() -> sinkB.deliveries.size() == 2);

            assertThat(sinkB.eventIds()).containsExactly("e3", "e4");
            assertThat(sinkA.eventIds()).containsExactly("e1", "e2", "e3"); // A 不再增长也不回退
        }
    }
}
