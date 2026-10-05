package com.github.cuixudong74.eventstreamrouterjava.model;

import java.util.Map;

public record RouteDefinition(
        String routeId,
        String eventType,
        Map<String, String> payloadEquals,
        String destinationId
) {
}
