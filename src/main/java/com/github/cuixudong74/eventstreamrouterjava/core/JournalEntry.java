package com.github.cuixudong74.eventstreamrouterjava.core;

import java.util.List;
import java.util.Map;

/**
 * 已提交位点的日志记录：路由决策与转换结果在写入时确定，重放结果与首次一致。
 */
public record JournalEntry(
        long seq,
        String eventId,
        String partitionKey,
        String eventType,
        int sourceSchemaVersion,
        int targetSchemaVersion,
        Map<String, Object> convertedPayload,
        List<ConversionIssue> conversionIssues,
        long ruleSetVersion,
        List<String> matchedDownstreams,
        String dropReason,
        long journaledAtMillis) {
}
