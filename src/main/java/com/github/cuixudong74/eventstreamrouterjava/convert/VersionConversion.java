package com.github.cuixudong74.eventstreamrouterjava.convert;

import java.util.List;

/**
 * 相邻两个结构版本之间的转换规则。
 */
public record VersionConversion(int fromVersion, int toVersion, List<FieldMapping> mappings) {
}
