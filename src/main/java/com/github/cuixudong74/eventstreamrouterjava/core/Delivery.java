package com.github.cuixudong74.eventstreamrouterjava.core;

import java.util.Map;

/**
 * 投递给下游的事件。同一分区键按 seq 顺序投递；eventId + seq 供下游去重。
 */
public record Delivery(
        String downstreamId,
        long seq,
        String eventId,
        String partitionKey,
        String eventType,
        int schemaVersion,
        Map<String, Object> payload,
        long attempt) {
}
