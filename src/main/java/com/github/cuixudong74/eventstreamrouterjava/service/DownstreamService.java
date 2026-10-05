package com.github.cuixudong74.eventstreamrouterjava.service;

import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamDispatcher;
import com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy;
import com.github.cuixudong74.eventstreamrouterjava.model.DownstreamMetrics;
import com.github.cuixudong74.eventstreamrouterjava.model.DownstreamState;
import com.github.cuixudong74.eventstreamrouterjava.persistence.DownstreamProgress;
import com.github.cuixudong74.eventstreamrouterjava.persistence.FileDurableEventStore;

import java.util.List;

public class DownstreamService {
    static final int DEFAULT_CAPACITY = 1024;

    private final FileDurableEventStore store;
    private final DownstreamDispatcher dispatcher;

    public DownstreamService(FileDurableEventStore store, DownstreamDispatcher dispatcher) {
        this.store = store;
        this.dispatcher = dispatcher;
    }

    public void add(String id, String startMode, Integer queueCapacity, BackpressurePolicy policy) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("downstream id is required");
        }
        int capacity = queueCapacity == null ? DEFAULT_CAPACITY : queueCapacity;
        if (capacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive");
        }

        long startSequence;
        String mode = startMode == null || startMode.isBlank() ? "BEGINNING" : startMode;
        if ("BEGINNING".equalsIgnoreCase(mode)) {
            startSequence = 1;
        } else if ("END".equalsIgnoreCase(mode)) {
            startSequence = store.nextSequence();
        } else {
            startSequence = Long.parseLong(mode);
            if (startSequence <= 0) {
                throw new IllegalArgumentException("start sequence must be positive");
            }
        }

        dispatcher.addDownstream(new DownstreamProgress(id, startSequence, startSequence - 1,
                capacity, policy == null ? BackpressurePolicy.DEAD_LETTER : policy,
                DownstreamState.ACTIVE, 0, 0));
    }

    public void remove(String id) throws InterruptedException {
        dispatcher.remove(id);
    }

    public void replay(String id, long fromSequence) {
        if (fromSequence <= 0) {
            throw new IllegalArgumentException("replay sequence must be positive");
        }
        dispatcher.replay(id, fromSequence);
    }

    public List<DownstreamMetrics> metrics() {
        return dispatcher.metrics();
    }
}
