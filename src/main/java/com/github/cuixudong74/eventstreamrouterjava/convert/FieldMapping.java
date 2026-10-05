package com.github.cuixudong74.eventstreamrouterjava.convert;

/**
 * 单字段转换规则。transform 决定如何由源字段得到目标字段。
 */
public record FieldMapping(String targetField, String sourceField, Transform transform, String constantValue) {

    public enum Transform { COPY, RENAME, INT_TO_STRING, STRING_TO_INT, CONSTANT, DROP }
}
