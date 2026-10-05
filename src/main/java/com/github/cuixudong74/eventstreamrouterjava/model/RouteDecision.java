package com.github.cuixudong74.eventstreamrouterjava.model;

public record RouteDecision(
        long sequence,
        String ruleVersion,
        String routeId,
        String destinationId
) {
}
