package com.github.cuixudong74.eventstreamrouterjava.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.ValidationError;
import com.github.cuixudong74.eventstreamrouterjava.model.ValidationException;

import java.util.ArrayList;
import java.util.List;

public class EventValidator {
    private final SchemaRegistry schemaRegistry;

    public EventValidator(SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public void validate(EventEnvelope event) {
        if (event == null) {
            throw new ValidationException(List.of(new ValidationError("$", "REQUIRED", "request body is required")));
        }

        List<ValidationError> errors = new ArrayList<>();
        requiredString(event.eventId(), "eventId", errors);
        requiredString(event.eventType(), "eventType", errors);
        requiredString(event.version(), "version", errors);
        requiredString(event.partitionKey(), "partitionKey", errors);

        JsonNode payload = event.payload();
        if (payload == null || payload.isNull()) {
            errors.add(new ValidationError("payload", "REQUIRED", "payload is required"));
        } else if (!payload.isObject()) {
            errors.add(new ValidationError("payload", "TYPE", "payload must be an object"));
        }

        if (event.version() != null && !event.version().isBlank()) {
            EventSchema schema = schemaRegistry.find(event.version()).orElse(null);
            if (schema == null) {
                errors.add(new ValidationError("version", "UNSUPPORTED_VERSION",
                        "version " + event.version() + " is not supported"));
            } else if (payload != null && payload.isObject()) {
                schema.validatePayload(payload, errors);
            }
        }

        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }
    }

    private static void requiredString(String value, String field, List<ValidationError> errors) {
        if (value == null || value.isBlank()) {
            errors.add(new ValidationError(field, "REQUIRED", field + " is required"));
        }
    }
}
