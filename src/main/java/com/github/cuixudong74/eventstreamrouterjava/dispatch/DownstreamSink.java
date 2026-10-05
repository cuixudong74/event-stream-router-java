package com.github.cuixudong74.eventstreamrouterjava.dispatch;

import com.github.cuixudong74.eventstreamrouterjava.core.Delivery;

/**
 * 下游消费方抽象。投递允许重试（可能重复），下游按 eventId 去重。
 */
public interface DownstreamSink {

    void deliver(Delivery delivery) throws Exception;
}
