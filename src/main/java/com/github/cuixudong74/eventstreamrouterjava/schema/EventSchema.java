package com.github.cuixudong74.eventstreamrouterjava.schema;

import java.util.List;

/**
 * 一个版本的事件结构。allowExtraFields=false 时未知字段也算校验失败。
 */
public record EventSchema(int version, List<SchemaField> fields, boolean allowExtraFields) {
}
