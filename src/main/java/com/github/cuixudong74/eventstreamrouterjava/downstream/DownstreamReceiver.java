package com.github.cuixudong74.eventstreamrouterjava.downstream;

import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;

public interface DownstreamReceiver {
    String destinationId();
    void receive(EventEnvelope event, RouteDecision decision);
}
