package com.github.cuixudong74.eventstreamrouterjava.core;

/**
 * 定位到具体字段的校验错误。
 */
public record FieldError(String field, String code, String message) {
}
