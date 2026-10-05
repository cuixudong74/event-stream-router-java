package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingRule;
import com.github.cuixudong74.eventstreamrouterjava.schema.EventSchema;
import com.github.cuixudong74.eventstreamrouterjava.schema.FieldType;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaField;
import com.github.cuixudong74.eventstreamrouterjava.service.IngestException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * 慢下游与背压：单个下游变慢不阻塞其他下游；滞后超上界时按策略拒绝接入且原因可解释。
 */
class BackpressureTest {

    @TempDir
    Path dir;

    @Test
    void slowDownstreamDoesNotBlockOthersAndTriggersExplainableBackpressure() {
        long maxLag = 3;
        try (TestHarness h = new TestHarness(dir, maxLag)) {
            h.schemas.register(new EventSchema(1, List.of(
                    new SchemaField("orderId", FieldType.STRING, true)), true));

            TestHarness.RecordingSink slowSink = new TestHarness.RecordingSink();
            slowSink.delayMillis = 300; // 慢下游
            TestHarness.RecordingSink fastSink = new TestHarness.RecordingSink();
            h.downstreams.registerSink("slow", slowSink);
            h.downstreams.registerSink("fast", fastSink);
            h.downstreams.addDownstream("SLOW", "slow", Optional.empty());
            h.downstreams.addDownstream("FAST", "fast", Optional.empty());
            h.routing.activate(List.of(
                    new RoutingRule("all", null, null, List.of("SLOW", "FAST"), 1)),
                    h.journal.lastSeq() + 1);

            // 持续提交直到触发背压
            int submitted = 0;
            IngestException backpressure = null;
            for (int i = 0; i < 50; i++) {
                try {
                    h.submit("e" + i, 1, "k1", "order", Map.of("orderId", "o" + i));
                    submitted++;
                } catch (IngestException e) {
                    backpressure = e;
                    break;
                }
                // 给慢下游制造滞后
                try { Thread.sleep(30); } catch (InterruptedException ignored) { }
            }

            // 背压触发，原因可解释：指明哪个下游、滞后量、策略
            assertThat(backpressure).isNotNull();
            assertThat(backpressure.status()).isEqualTo(429);
            assertThat(backpressure.getMessage())
                    .contains("SLOW").contains("maxLag").contains("REJECT_INGEST");

            // 快下游不受慢下游影响：已收到全部已提交事件
            int finalSubmitted = submitted;
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> fastSink.deliveries.size() == finalSubmitted);

            // 慢下游仍在追赶，未丢事件
            await().atMost(15, TimeUnit.SECONDS)
                    .until(() -> slowSink.deliveries.size() == finalSubmitted);
        }
    }

    @Test
    void lagIsObservable() {
        try (TestHarness h = new TestHarness(dir)) {
            h.schemas.register(new EventSchema(1, List.of(
                    new SchemaField("orderId", FieldType.STRING, true)), true));
            TestHarness.RecordingSink sink = new TestHarness.RecordingSink();
            sink.delayMillis = 500;
            h.downstreams.registerSink("slow", sink);
            h.downstreams.addDownstream("SLOW", "slow", Optional.empty());
            h.routing.activate(List.of(new RoutingRule("all", null, null, List.of("SLOW"), 1)),
                    h.journal.lastSeq() + 1);

            for (int i = 0; i < 5; i++) {
                h.submit("e" + i, 1, "k1", "order", Map.of("orderId", "o" + i));
            }
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> h.downstreams.lagSnapshot().get("SLOW") > 0);
            assertThat(h.downstreams.lagSnapshot()).containsKey("SLOW");
        }
    }
}
