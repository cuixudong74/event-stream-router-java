package com.github.cuixudong74.eventstreamrouterjava.routing;

import java.util.List;

/**
 * 一条路由规则：eventType 精确匹配（null 表示匹配全部）+ 可选分区键前缀，
 * 命中后投递到 downstreamIds。priority 小的先评估，先中先得。
 */
public record RoutingRule(String ruleId, String eventType, String partitionKeyPrefix,
                          List<String> downstreamIds, int priority) {

    public boolean matches(String eventType, String partitionKey) {
        if (this.eventType != null && !this.eventType.equals(eventType)) {
            return false;
        }
        if (this.partitionKeyPrefix != null
                && (partitionKey == null || !partitionKey.startsWith(this.partitionKeyPrefix))) {
            return false;
        }
        return true;
    }
}
