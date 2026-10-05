package com.github.cuixudong74.eventstreamrouterjava.dispatch;

/**
 * 一个已注册下游：从明确的 startSeq 开始消费。
 */
public record Downstream(String id, String sinkName, long startSeq, long registeredAtMillis) {
}
