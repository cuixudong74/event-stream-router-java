package com.github.cuixudong74.eventstreamrouterjava.model;

public record DownstreamMetrics(
        String downstreamId,
        String state,
        long startSequence,
        long committedSequence,
        long latestSequence,
        int queueSize,
        boolean inFlight,
        Long inFlightSequence,
        long lagCount,
        long deliveredCount,
        long deadLetterCount
) {
}
