package com.github.cuixudong74.eventstreamrouterjava.service;

import com.github.cuixudong74.eventstreamrouterjava.core.ConversionIssue;

import java.util.List;

/**
 * 接入结果：分配的位点、命中的下游、转换问题（如有）。
 */
public record IngestResult(long seq, long ruleSetVersion, List<String> matchedDownstreams,
                           List<ConversionIssue> conversionIssues, String dropReason) {
}
