package com.github.cuixudong74.eventstreamrouterjava.web;

import com.fasterxml.jackson.databind.JsonNode;

public record SubmitEventRequest(
        String eventId,
        String eventType,
        String version,
        String partitionKey,
        JsonNode payload
) {
}
