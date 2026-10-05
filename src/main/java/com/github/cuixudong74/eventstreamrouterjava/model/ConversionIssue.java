package com.github.cuixudong74.eventstreamrouterjava.model;

public record ConversionIssue(
        String sourceField,
        String targetField,
        String code,
        String reason
) {
}
