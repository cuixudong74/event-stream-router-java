package com.github.cuixudong74.eventstreamrouterjava.persistence;

import com.github.cuixudong74.eventstreamrouterjava.model.ConversionIssue;
import com.github.cuixudong74.eventstreamrouterjava.model.ConversionStatus;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;

import java.util.List;

public record StoredEvent(
        long sequence,
        EventEnvelope originalEvent,
        EventEnvelope targetEvent,
        RouteDecision decision,
        ConversionStatus conversionStatus,
        List<ConversionIssue> conversionIssues
) {
}
