package com.github.cuixudong74.eventstreamrouterjava.model;

import java.time.Instant;

public record DeadLetterRecord(
        long sequence,
        String eventId,
        String downstreamId,
        String reason,
        Instant createdAt
) {
}
