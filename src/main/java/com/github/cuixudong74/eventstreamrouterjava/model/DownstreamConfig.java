package com.github.cuixudong74.eventstreamrouterjava.model;

public record DownstreamConfig(
        String id,
        long startSequence,
        int queueCapacity,
        BackpressurePolicy backpressurePolicy,
        DownstreamState state
) {
}
