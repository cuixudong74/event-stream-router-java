package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;
import com.github.cuixudong74.eventstreamrouterjava.tests.support.RouterTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderingAndReplayTest {
    @TempDir
    Path directory;

    @Test
    void preservesPartitionOrderAndReplaysSameEventsInSameOrder() throws Exception {
        List<EventEnvelope> received = new ArrayList<>();
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            harness.downstreams.add("orders", "BEGINNING", 10,
                    com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("orders", new com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamReceiver() {
                @Override
                public String destinationId() {
                    return "orders";
                }

                @Override
                public void receive(EventEnvelope event, RouteDecision decision) {
                    synchronized (received) {
                        received.add(event);
                        if (received.size() < 3) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                }
            });

            for (int i = 1; i <= 3; i++) {
                harness.router.submit(harness.objectMapper.readTree("""
                        {
                          "eventId":"e-%d",
                          "eventType":"order.created",
                          "version":"2",
                          "partitionKey":"customer-1",
                          "payload":{"amount":{"value":%d,"currency":"USD"},"customerType":"RETAIL","occurredAt":"2026-01-01T00:0%d:00Z"}
                        }
                        """.formatted(i, i, i)));
            }
            waitForSize(received, 3);
            assertEquals(List.of("e-1", "e-2", "e-3"),
                    received.stream().map(EventEnvelope::eventId).toList());

            harness.downstreams.replay("orders", 1);
            waitForSize(received, 6);
            List<String> replay = received.subList(3, 6).stream().map(EventEnvelope::eventId).toList();
            assertEquals(List.of("e-1", "e-2", "e-3"), replay);
            assertEquals(3, new LinkedHashSet<>(received.stream().map(EventEnvelope::eventId).toList()).size());
        }
    }

    private static void waitForSize(List<EventEnvelope> events, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            synchronized (events) {
                if (events.size() >= expected) {
                    return;
                }
            }
            Thread.sleep(10);
        }
        throw new AssertionError("timed out waiting for " + expected + " events, got " + events.size());
    }
}
