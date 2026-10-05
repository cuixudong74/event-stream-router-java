package com.github.cuixudong74.eventstreamrouterjava.model;

import java.util.List;

public record SubmitResult(
        long sequence,
        String eventId,
        String destinationId,
        String ruleVersion,
        ConversionStatus conversionStatus,
        List<ConversionIssue> conversionIssues
) {
}
