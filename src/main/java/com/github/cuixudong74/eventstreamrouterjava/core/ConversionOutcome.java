package com.github.cuixudong74.eventstreamrouterjava.core;

import java.util.List;
import java.util.Map;

/**
 * 版本转换结果。status 区分：CONVERTED 全部成功；PARTIAL 部分字段无法转换（见 issues）；
 * FAILED 整体无法转换（例如没有转换路径）。
 */
public record ConversionOutcome(Status status, Map<String, Object> payload, List<ConversionIssue> issues) {

    public enum Status { CONVERTED, PARTIAL, FAILED }

    public static ConversionOutcome converted(Map<String, Object> payload) {
        return new ConversionOutcome(Status.CONVERTED, payload, List.of());
    }

    public static ConversionOutcome partial(Map<String, Object> payload, List<ConversionIssue> issues) {
        return new ConversionOutcome(Status.PARTIAL, payload, List.copyOf(issues));
    }

    public static ConversionOutcome failed(List<ConversionIssue> issues) {
        return new ConversionOutcome(Status.FAILED, Map.of(), List.copyOf(issues));
    }
}
