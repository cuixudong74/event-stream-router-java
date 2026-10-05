package com.github.cuixudong74.eventstreamrouterjava.model;

import java.util.List;

public record RuleVersion(
        String ruleVersion,
        long startsAtSequence,
        List<RouteDefinition> routes,
        String defaultDestinationId
) {
}
