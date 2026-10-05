package com.github.cuixudong74.eventstreamrouterjava.persistence;

import com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy;
import com.github.cuixudong74.eventstreamrouterjava.model.DownstreamState;

public record DownstreamProgress(
        String downstreamId,
        long startSequence,
        long committedSequence,
        int queueCapacity,
        BackpressurePolicy backpressurePolicy,
        DownstreamState state,
        long deliveredCount,
        long deadLetterCount
) {
}
