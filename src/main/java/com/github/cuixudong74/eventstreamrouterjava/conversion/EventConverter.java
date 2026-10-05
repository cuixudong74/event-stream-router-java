package com.github.cuixudong74.eventstreamrouterjava.conversion;

import com.github.cuixudong74.eventstreamrouterjava.model.ConversionIssue;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;

import java.util.List;

public interface EventConverter {
    String sourceVersion();

    String targetVersion();

    EventEnvelope convert(EventEnvelope event);

    List<ConversionIssue> issues(EventEnvelope event);
}
