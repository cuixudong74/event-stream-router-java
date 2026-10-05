package com.github.cuixudong74.eventstreamrouterjava.model;

import java.util.List;

public record RouterMetrics(
        long nextSequence,
        long acceptedCount,
        long rejectedCount,
        long deadLetterCount,
        List<DownstreamMetrics> downstreams
) {
}
