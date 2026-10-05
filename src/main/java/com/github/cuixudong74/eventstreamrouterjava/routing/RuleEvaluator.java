package com.github.cuixudong74.eventstreamrouterjava.routing;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDefinition;
import com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion;

import java.util.Map;

public class RuleEvaluator {
    public RouteDecision evaluate(EventEnvelope event, RuleVersion ruleVersion) {
        if (event.sequence() == null) {
            throw new IllegalArgumentException("event sequence is required for routing");
        }

        for (RouteDefinition route : ruleVersion.routes()) {
            if (matches(event, route)) {
                return new RouteDecision(event.sequence(), ruleVersion.ruleVersion(),
                        route.routeId(), route.destinationId());
            }
        }

        if (ruleVersion.defaultDestinationId() == null || ruleVersion.defaultDestinationId().isBlank()) {
            return null;
        }
        return new RouteDecision(event.sequence(), ruleVersion.ruleVersion(),
                "__default__", ruleVersion.defaultDestinationId());
    }

    private boolean matches(EventEnvelope event, RouteDefinition route) {
        if (!event.eventType().equals(route.eventType())) {
            return false;
        }
        for (Map.Entry<String, String> condition : route.payloadEquals().entrySet()) {
            JsonNode actual = event.payload().path(condition.getKey());
            if (actual.isMissingNode() || !actual.asText().equals(condition.getValue())) {
                return false;
            }
        }
        return true;
    }
}
