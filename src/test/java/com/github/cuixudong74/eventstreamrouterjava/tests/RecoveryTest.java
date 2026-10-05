package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;
import com.github.cuixudong74.eventstreamrouterjava.tests.support.RouterTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecoveryTest {
    @TempDir
    Path directory;

    @Test
    void restoresCommittedAndInFlightStatesWithoutSkippingEvents() throws Exception {
        var firstDeliveryStarted = new CountDownLatch(1);
        var releaseFirstDelivery = new CountDownLatch(1);
        var firstAttempt = new ArrayList<EventEnvelope>();

        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            harness.downstreams.add("payments", "BEGINNING", 10, BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("payments", new com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamReceiver() {
                @Override
                public String destinationId() {
                    return "payments";
                }

                @Override
                public void receive(EventEnvelope event, RouteDecision decision) {
                    firstAttempt.add(event);
                    firstDeliveryStarted.countDown();
                    try {
                        releaseFirstDelivery.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("simulated process restart during delivery", e);
                    }
                }
            });

            harness.router.submit(event("e-1", "BUSINESS"));
            assertTrue(firstDeliveryStarted.await(2, java.util.concurrent.TimeUnit.SECONDS));
            harness.router.submit(event("e-2", "BUSINESS"));
            assertTrue(harness.store.read(2L).isPresent());
            assertEquals(0L, harness.store.progress("payments").committedSequence());
            harness.dispatcher.stop();
        }

        var recovered = new ArrayList<EventEnvelope>();
        try (RouterTestHarness harness = RouterTestHarness.reopen(directory)) {
            harness.register("payments", RouterTestHarness.receiver("payments", recovered));
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (System.nanoTime() < deadline && recovered.size() < 2) {
                Thread.sleep(10);
            }
            assertEquals(List.of("e-1", "e-2"), recovered.stream().map(EventEnvelope::eventId).toList());
            assertEquals(2L, harness.store.progress("payments").committedSequence());
            var first = harness.store.read(1L).orElseThrow();
            assertEquals(first.decision(), first.originalEvent() != null ? first.decision() : null);
            assertEquals("rules-v1", first.decision().ruleVersion());
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
                          "partitionKey":"account-1",
                          "payload":{"amount":{"value":10,"currency":"USD"},"customerType":"%s","occurredAt":"2026-01-01T00:00:00Z"}
                        }
                        """.formatted(id, customerType));
    }
}
