package com.github.cuixudong74.eventstreamrouterjava.routing;

import java.util.List;

/**
 * 一个规则集版本，从 effectiveFromSeq（含）开始生效。
 */
public record RuleSetVersion(long version, long effectiveFromSeq, List<RoutingRule> rules, long createdAtMillis) {
}
