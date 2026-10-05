package com.github.cuixudong74.eventstreamrouterjava.routing;

import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;
import com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion;

public class RoutingService {
    private final RuleCatalog ruleCatalog;
    private final RuleEvaluator ruleEvaluator;

    public RoutingService(RuleCatalog ruleCatalog, RuleEvaluator ruleEvaluator) {
        this.ruleCatalog = ruleCatalog;
        this.ruleEvaluator = ruleEvaluator;
    }

    public RouteDecision route(EventEnvelope event) {
        long sequence = event.sequence();
        RuleVersion ruleVersion = ruleCatalog.effectiveAt(sequence)
                .orElseThrow(() -> new IllegalStateException("no routing rule effective at sequence " + sequence));
        RouteDecision decision = ruleEvaluator.evaluate(event, ruleVersion);
        if (decision == null) {
            throw new IllegalStateException("event " + event.eventId()
                    + " at sequence " + sequence + " matched no route");
        }
        return decision;
    }
}
