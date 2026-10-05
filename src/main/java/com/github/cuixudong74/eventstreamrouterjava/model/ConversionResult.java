package com.github.cuixudong74.eventstreamrouterjava.model;

import java.util.List;

public record ConversionResult(
        EventEnvelope targetEvent,
        ConversionStatus status,
        List<ConversionIssue> issues
) {
}
