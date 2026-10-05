package com.github.cuixudong74.eventstreamrouterjava.model;

import com.fasterxml.jackson.databind.JsonNode;

public record EventEnvelope(
        String eventId,
        String eventType,
        String version,
        String partitionKey,
        JsonNode payload,
        Long sequence
) {
    public EventEnvelope {
        if (sequence != null && sequence <= 0) {
            throw new IllegalArgumentException("sequence must be positive");
        }
    }

    public EventEnvelope(String eventId, String eventType, String version,
                         String partitionKey, JsonNode payload) {
        this(eventId, eventType, version, partitionKey, payload, null);
    }

    public EventEnvelope withSequence(long newSequence) {
        return new EventEnvelope(eventId, eventType, version, partitionKey,
                payload == null ? null : payload.deepCopy(), newSequence);
    }
}
