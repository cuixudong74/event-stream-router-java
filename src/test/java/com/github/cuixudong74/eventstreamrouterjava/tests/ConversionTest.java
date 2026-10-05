package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.conversion.ConversionRegistry;
import com.github.cuixudong74.eventstreamrouterjava.conversion.EventConversionService;
import com.github.cuixudong74.eventstreamrouterjava.conversion.V1ToV2EventConverter;
import com.github.cuixudong74.eventstreamrouterjava.model.ConversionStatus;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.tests.support.RouterTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ConversionTest {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

    @TempDir
    Path directory;

    @Test
    void convertsV1ToV2DeterministicallyAndMarksMissingField() throws Exception {
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            var request = objectMapper.readTree("""
                    {
                      "eventId":"e-1",
                      "eventType":"order.created",
                      "version":"1",
                      "partitionKey":"p-1",
                      "payload":{"amount":42,"currency":"USD","customerType":"RETAIL"}
                    }
                    """);
            harness.downstreams.add("orders", "BEGINNING", 10,
                    com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy.BLOCK_PRODUCER);
            harness.register("orders", RouterTestHarness.receiver("orders", List.of()));

            var result = harness.router.submit(request);

            assertEquals(ConversionStatus.PARTIAL, result.conversionStatus());
            assertEquals("UNMAPPABLE_MISSING_FIELD", result.conversionIssues().get(0).code());
            assertEquals("payload.occurredAt", result.conversionIssues().get(0).targetField());
            var stored = harness.store.read(1L).orElseThrow();
            assertEquals("2", stored.targetEvent().version());
            assertEquals(42, stored.targetEvent().payload().path("amount").path("value").asInt());
            assertEquals("USD", stored.targetEvent().payload().path("amount").path("currency").asText());
            assertFalse(stored.targetEvent().payload().has("occurredAt"));
        }
    }

    @Test
    void sameSourceEventConvertsToSameTargetPayloadEveryTime() throws Exception {
        EventEnvelope event = objectMapper.convertValue(objectMapper.readTree("""
                {
                  "eventId":"e-2",
                  "eventType":"order.created",
                  "version":"1",
                  "partitionKey":"p-1",
                  "payload":{"amount":7,"currency":"EUR","customerType":"VIP"}
                }
                """), EventEnvelope.class);

        var first = new EventConversionService(new ConversionRegistry(List.of(new V1ToV2EventConverter()))).convert(event);
        var second = new EventConversionService(new ConversionRegistry(List.of(new V1ToV2EventConverter()))).convert(event);

        assertEquals(first.status(), second.status());
        assertEquals(first.issues(), second.issues());
        assertEquals(objectMapper.writeValueAsString(first.targetEvent().payload()),
                objectMapper.writeValueAsString(second.targetEvent().payload()));
    }
}
