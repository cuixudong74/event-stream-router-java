package com.github.cuixudong74.eventstreamrouterjava.core;

/**
 * 单个字段在版本转换中遇到的问题，可区分处理。
 */
public record ConversionIssue(String field, String code, String detail) {

    public static final String MISSING_SOURCE = "MISSING_SOURCE";
    public static final String INCOMPATIBLE_VALUE = "INCOMPATIBLE_VALUE";
    public static final String NO_RULE = "NO_RULE";
}
