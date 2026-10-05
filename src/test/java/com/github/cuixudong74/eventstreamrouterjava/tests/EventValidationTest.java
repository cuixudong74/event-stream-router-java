package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.model.ValidationException;
import com.github.cuixudong74.eventstreamrouterjava.tests.support.RouterTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventValidationTest {
    @TempDir
    Path directory;

    @Test
    void rejectsUnknownVersionWithFieldSpecificReason() throws Exception {
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            ValidationException exception = assertThrows(ValidationException.class,
                    () -> harness.router.submit(harness.objectMapper.readTree("""
                            {
                              "eventId":"e-1",
                              "eventType":"order.created",
                              "version":"999",
                              "partitionKey":"p-1",
                              "payload":{"amount":10,"currency":"USD"}
                            }
                            """)));

            assertEquals("version", exception.getErrors().get(0).field());
            assertEquals("UNSUPPORTED_VERSION", exception.getErrors().get(0).code());
            assertEquals(1, harness.store.nextSequence());
        }
    }

    @Test
    void reportsAllStructuralProblemsWithoutDefaultingFields() throws Exception {
        try (RouterTestHarness harness = RouterTestHarness.create(directory)) {
            ValidationException exception = assertThrows(ValidationException.class,
                    () -> harness.router.submit(harness.objectMapper.readTree("""
                            {
                              "eventId":"",
                              "eventType":"order.created",
                              "version":"2",
                              "partitionKey":"p-1",
                              "payload":{"amount":{"value":"bad","currency":"usd"},"unexpected":true}
                            }
                            """)));

            assertTrue(exception.getErrors().stream()
                    .anyMatch(error -> error.field().equals("eventId") && error.code().equals("REQUIRED")));
            assertTrue(exception.getErrors().stream()
                    .anyMatch(error -> error.field().equals("payload.amount.value") && error.code().equals("TYPE")));
            assertTrue(exception.getErrors().stream()
                    .anyMatch(error -> error.field().equals("payload.amount.currency") && error.code().equals("FORMAT")));
            assertTrue(exception.getErrors().stream()
                    .anyMatch(error -> error.field().equals("payload.occurredAt") && error.code().equals("REQUIRED")));
            assertTrue(exception.getErrors().stream()
                    .anyMatch(error -> error.field().equals("payload.unexpected") && error.code().equals("UNKNOWN_FIELD")));
            assertEquals(1, harness.store.nextSequence());
        }
    }
}
