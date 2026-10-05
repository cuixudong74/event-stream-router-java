package com.github.cuixudong74.eventstreamrouterjava.conversion;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ConversionRegistry {
    private final Map<String, EventConverter> converters = new HashMap<>();

    public ConversionRegistry() {
    }

    public ConversionRegistry(Iterable<EventConverter> initialConverters) {
        initialConverters.forEach(this::register);
    }

    public void register(EventConverter converter) {
        String key = key(converter.sourceVersion(), converter.targetVersion());
        EventConverter existing = converters.putIfAbsent(key, converter);
        if (existing != null && !existing.getClass().equals(converter.getClass())) {
            throw new IllegalStateException("conversion rule already exists: " + key);
        }
    }

    public Optional<EventConverter> find(String sourceVersion, String targetVersion) {
        return Optional.ofNullable(converters.get(key(sourceVersion, targetVersion)));
    }

    private static String key(String sourceVersion, String targetVersion) {
        return sourceVersion + "->" + targetVersion;
    }
}
