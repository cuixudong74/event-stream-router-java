package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.core.Delivery;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingRule;
import com.github.cuixudong74.eventstreamrouterjava.schema.EventSchema;
import com.github.cuixudong74.eventstreamrouterjava.schema.FieldType;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 中断与重启恢复：已提交位点与未完成投递可区分；恢复不跳过事件；
 * 允许的重复可按事件标识去重；重启后不留下无法解释的中间状态。
 */
class RecoveryTest {

    @TempDir
    Path dir;

    @Test
    void restartResumesWithoutSkippingEvents() {
        TestHarness.RecordingSink sink1 = new TestHarness.RecordingSink();
        sink1.delayMillis = 150; // 让崩溃发生在投递中途

        long committedBeforeCrash;
        try (TestHarness h1 = new TestHarness(dir)) {
            h1.schemas.register(new EventSchema(1, List.of(
                    new SchemaField("orderId", FieldType.STRING, true)), true));
            h1.downstreams.registerSink("sinkA", sink1);
            h1.downstreams.addDownstream("A", "sinkA", Optional.empty());
            h1.routing.activate(List.of(new RoutingRule("all", null, null, List.of("A"), 1)),
                    h1.journal.lastSeq() + 1);

            for (int i = 1; i <= 5; i++) {
                h1.submit("e" + i, 1, "k1", "order", Map.of("orderId", String.valueOf(i)));
            }
            // 等到部分投递完成，模拟崩溃
            await().atMost(5, TimeUnit.SECONDS).until(() -> sink1.deliveries.size() >= 1);
            committedBeforeCrash = h1.journal.lastSeq();
        } // close = 模拟进程停止

        assertThat(committedBeforeCrash).isEqualTo(5);

        // 重启：同一数据目录新建实例，已提交位点恢复
        TestHarness.RecordingSink sink2 = new TestHarness.RecordingSink();
        try (TestHarness h2 = new TestHarness(dir)) {
            assertThat(h2.journal.lastSeq()).isEqualTo(5);
            h2.downstreams.registerSink("sinkA", sink2);
            h2.downstreams.init(); // 恢复已注册下游的投递循环

            // 未完成的投递被补齐
            await().atMost(10, TimeUnit.SECONDS).until(() ->
                    sink1.deliveries.size() + sink2.deliveries.size() >= 5);

            // 合并两次运行的投递，按 (eventId, seq) 去重后必须等于首次提交顺序，无跳过
            List<Delivery> all = new ArrayList<>();
            all.addAll(sink1.deliveries);
            all.addAll(sink2.deliveries);
            Map<String, Long> deduped = new LinkedHashMap<>();
            all.stream().sorted((a, b) -> Long.compare(a.seq(), b.seq()))
                    .forEach(d -> deduped.putIfAbsent(d.eventId(), d.seq()));
            assertThat(deduped.keySet()).containsExactly("e1", "e2", "e3", "e4", "e5");

            // 检查点最终推进到已提交位点，无无法解释的中间状态
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    h2.checkpoints.checkpoint("A").orElse(0) == 5);

            // 重启后新事件位点连续，不重叠不跳号
            long seq = h2.submit("e6", 1, "k1", "order", Map.of("orderId", "6"));
            assertThat(seq).isEqualTo(6);
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    sink2.eventIds().contains("e6"));
        }
    }

    @Test
    void duplicateSubmissionAfterRetryIsDeduplicatable() {
        // 生产者因超时重发同一 eventId：两条日志记录各自有位点，
        // 下游按 eventId 去重，不会看到两份不同的业务数据
        TestHarness.RecordingSink sink = new TestHarness.RecordingSink();
        try (TestHarness h = new TestHarness(dir)) {
            h.schemas.register(new EventSchema(1, List.of(
                    new SchemaField("orderId", FieldType.STRING, true)), true));
            h.downstreams.registerSink("sinkA", sink);
            h.downstreams.addDownstream("A", "sinkA", Optional.empty());
            h.routing.activate(List.of(new RoutingRule("all", null, null, List.of("A"), 1)),
                    h.journal.lastSeq() + 1);

            h.submit("e1", 1, "k1", "order", Map.of("orderId", "1"));
            h.submit("e1", 1, "k1", "order", Map.of("orderId", "1")); // 生产者重发

            await().atMost(5, TimeUnit.SECONDS).until(() -> sink.deliveries.size() == 2);
            assertThat(sink.deliveries.stream().map(Delivery::eventId).distinct().count()).isEqualTo(1);
        }
    }
}
