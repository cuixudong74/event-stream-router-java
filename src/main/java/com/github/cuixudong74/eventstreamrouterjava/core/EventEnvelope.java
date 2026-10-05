package com.github.cuixudong74.eventstreamrouterjava.core;

import java.util.Map;

/**
 * 上游提交的原始事件。eventId 由生产者提供，用于下游去重。
 */
public record EventEnvelope(
        String eventId,
        int schemaVersion,
        String partitionKey,
        String eventType,
        Map<String, Object> payload,
        long submittedAtMillis) {
}
