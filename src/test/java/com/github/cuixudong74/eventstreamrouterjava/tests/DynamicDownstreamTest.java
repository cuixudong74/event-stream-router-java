package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamReceiver;
import com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;
import com.github.cuixudong74.eventstreamrouterjava.tests.support.RouterTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamicDownstreamTest {
    @TempDir
    Path directory;

    @Test
    void newDownstreamStartsAtExplicitCurrentEndAndProgressSurvivesHandlerSwap() throws Exception {
        List<EventEnvelope> firstReceiver = new ArrayList<>();
        List<EventEnvelope> replacementReceiver = new ArrayList<>();
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            harness.downstreams.add("orders", "BEGINNING", 10, BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("orders", RouterTestHarness.receiver("orders", firstReceiver));
            harness.router.submit(event("e-old", "RETAIL"));
            waitForSize(firstReceiver, 1);
            long committed = harness.store.progress("orders").committedSequence();
            assertEquals(1L, committed);

            harness.registry.unregister("orders");
            harness.register("orders", RouterTestHarness.receiver("orders", replacementReceiver));
            harness.router.submit(event("e-new", "RETAIL"));
            waitForSize(replacementReceiver, 1);
            assertEquals(2L, harness.store.progress("orders").committedSequence());
            assertNotEquals(0L, harness.store.progress("orders").committedSequence());

            List<EventEnvelope> late = new ArrayList<>();
            harness.downstreams.add("payments", "END", 10, BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("payments", RouterTestHarness.receiver("payments", late));
            harness.router.submit(event("e-late", "BUSINESS"));
            waitForSize(late, 1);
            assertEquals(List.of("e-late"), late.stream().map(EventEnvelope::eventId).toList());
            assertEquals(3L, harness.store.progress("payments").startSequence());
        }
    }

    @Test
    void removingDownstreamDrainsCurrentWorkAndMarksRemoved() throws Exception {
        List<EventEnvelope> received = new ArrayList<>();
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            harness.downstreams.add("orders", "BEGINNING", 10, BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("orders", RouterTestHarness.receiver("orders", received));
            harness.router.submit(event("e-1", "RETAIL"));
            waitForSize(received, 1);

            harness.downstreams.remove("orders");
            var progress = harness.store.progress("orders");
            assertEquals("REMOVED", progress.state().name());
            assertTrue(progress.committedSequence() >= 1L);
        }
    }

    private static com.fasterxml.jackson.databind.JsonNode event(String id, String customerType) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                .readTree("""
                        {
                          "eventId":"%s",
                          "eventType":"order.created",
                          "version":"2",
                          "partitionKey":"p-1",
                          "payload":{"amount":{"value":1,"currency":"USD"},"customerType":"%s","occurredAt":"2026-01-01T00:00:00Z"}
                        }
                        """.formatted(id, customerType));
    }

    private static void waitForSize(List<EventEnvelope> events, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (events.size() >= expected) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for " + expected + " events");
    }
}
