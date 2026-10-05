package com.github.cuixudong74.eventstreamrouterjava.conversion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.cuixudong74.eventstreamrouterjava.model.ConversionIssue;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;

import java.util.List;

public class V1ToV2EventConverter implements EventConverter {
    @Override
    public String sourceVersion() {
        return "1";
    }

    @Override
    public String targetVersion() {
        return "2";
    }

    @Override
    public EventEnvelope convert(EventEnvelope event) {
        ObjectNode source = (ObjectNode) event.payload();
        ObjectNode amount = source.objectNode();
        amount.set("value", source.path("amount").deepCopy());
        amount.set("currency", source.path("currency").deepCopy());

        ObjectNode target = source.objectNode();
        target.set("amount", amount);
        if (source.hasNonNull("customerType")) {
            target.set("customerType", source.path("customerType").deepCopy());
        }
        if (source.hasNonNull("occurredAt")) {
            target.set("occurredAt", source.path("occurredAt").deepCopy());
        }
        return new EventEnvelope(event.eventId(), event.eventType(), "2",
                event.partitionKey(), target, event.sequence());
    }

    @Override
    public List<ConversionIssue> issues(EventEnvelope event) {
        JsonNode occurredAt = event.payload().path("occurredAt");
        if (occurredAt.isMissingNode() || occurredAt.isNull()) {
            return List.of(new ConversionIssue(
                    "payload.occurredAt",
                    "payload.occurredAt",
                    "UNMAPPABLE_MISSING_FIELD",
                    "v2 requires occurredAt but v1 does not provide it; no default is inferred"));
        }
        return List.of();
    }
}
