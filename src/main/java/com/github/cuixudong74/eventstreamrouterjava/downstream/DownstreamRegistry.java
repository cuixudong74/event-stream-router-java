package com.github.cuixudong74.eventstreamrouterjava.downstream;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DownstreamRegistry {
    private final ConcurrentMap<String, DownstreamReceiver> receivers = new ConcurrentHashMap<>();

    public Optional<DownstreamReceiver> find(String destinationId) {
        return Optional.ofNullable(receivers.get(destinationId));
    }

    public void register(DownstreamReceiver receiver) {
        DownstreamReceiver existing = receivers.putIfAbsent(receiver.destinationId(), receiver);
        if (existing != null) {
            throw new IllegalArgumentException("downstream receiver already registered: " + receiver.destinationId());
        }
    }

    public void unregister(String destinationId) {
        receivers.remove(destinationId);
    }
}
