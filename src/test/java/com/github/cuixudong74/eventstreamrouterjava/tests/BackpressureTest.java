package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;
import com.github.cuixudong74.eventstreamrouterjava.service.BackpressureException;
import com.github.cuixudong74.eventstreamrouterjava.tests.support.RouterTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackpressureTest {
    @TempDir
    Path directory;

    @Test
    void rejectPolicyReturnsBackpressureAndDoesNotSilentlyDrop() throws Exception {
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            harness.downstreams.add("orders", "BEGINNING", 1, BackpressurePolicy.REJECT_PRODUCER);
            harness.register("orders", blockingReceiver("orders", started, release));

            harness.router.submit(event("e-1", "RETAIL"));
            assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
            BackpressureException exception = assertThrows(BackpressureException.class,
                    () -> harness.router.submit(event("e-2", "RETAIL")));
            assertTrue(exception.getMessage().contains("at capacity 1"));
            release.countDown();
        }
    }

    @Test
    void deadLetterPolicyRecordsOverloadedEventWithReason() throws Exception {
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            harness.downstreams.add("orders", "BEGINNING", 1, BackpressurePolicy.DEAD_LETTER);
            harness.register("orders", blockingReceiver("orders", started, release));

            harness.router.submit(event("e-1", "RETAIL"));
            assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));
            var result = harness.router.submit(event("e-2", "RETAIL"));
            assertEquals(2L, result.sequence());

            var deadLetter = harness.store.deadLetters().get(0);
            assertEquals(2L, deadLetter.sequence());
            assertEquals("orders", deadLetter.downstreamId());
            assertTrue(deadLetter.reason().contains("capacity"));
            release.countDown();
        }
    }

    @Test
    void blockingSlowDownstreamDoesNotBlockIndependentFastDownstream() throws Exception {
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            var started = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var payments = new ArrayList<EventEnvelope>();
            harness.downstreams.add("orders", "BEGINNING", 1, BackpressurePolicy.BLOCK_PRODUCER);
            harness.downstreams.add("payments", "BEGINNING", 1, BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("orders", blockingReceiver("orders", started, release));
            harness.register("payments", RouterTestHarness.receiver("payments", payments));

            harness.router.submit(event("e-1", "RETAIL"));
            assertTrue(started.await(2, java.util.concurrent.TimeUnit.SECONDS));

            harness.router.submit(event("e-2", "BUSINESS"));
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (System.nanoTime() < deadline && payments.isEmpty()) {
                Thread.sleep(10);
            }
            assertEquals(List.of("e-2"), payments.stream().map(EventEnvelope::eventId).toList());
            release.countDown();
        }
    }

    private static com.fasterxml.jackson.databind.JsonNode event(String id, String customerType) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper().registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule()).readTree("""
                {
                  "eventId":"%s",
                  "eventType":"order.created",
                  "version":"2",
                  "partitionKey":"p-1",
                  "payload":{"amount":{"value":1,"currency":"USD"},"customerType":"%s","occurredAt":"2026-01-01T00:00:00Z"}
                }
                """.formatted(id, customerType));
    }

    private static com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamReceiver blockingReceiver(
            String id, CountDownLatch started, CountDownLatch release) {
        var calls = new AtomicInteger();
        return new com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamReceiver() {
            @Override
            public String destinationId() {
                return id;
            }

            @Override
            public void receive(EventEnvelope event, RouteDecision decision) {
                if (calls.getAndIncrement() == 0) {
                    started.countDown();
                    try {
                        release.await(5, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        };
    }
}
