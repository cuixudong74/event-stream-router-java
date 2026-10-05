package com.github.cuixudong74.eventstreamrouterjava.conversion;

import com.github.cuixudong74.eventstreamrouterjava.model.ConversionIssue;
import com.github.cuixudong74.eventstreamrouterjava.model.ConversionResult;
import com.github.cuixudong74.eventstreamrouterjava.model.ConversionStatus;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;

import java.util.List;

public class EventConversionService {
    private static final String TARGET_VERSION = "2";

    private final ConversionRegistry registry;

    public EventConversionService(ConversionRegistry registry) {
        this.registry = registry;
    }

    public ConversionResult convert(EventEnvelope event) {
        if (TARGET_VERSION.equals(event.version())) {
            return new ConversionResult(copy(event), ConversionStatus.CONVERTED, List.of());
        }

        EventConverter converter = registry.find(event.version(), TARGET_VERSION)
                .orElseThrow(() -> new IllegalStateException(
                        "no conversion rule from version " + event.version() + " to " + TARGET_VERSION));
        EventEnvelope target = converter.convert(event);
        List<ConversionIssue> issues = List.copyOf(converter.issues(event));
        ConversionStatus status = issues.isEmpty() ? ConversionStatus.CONVERTED : ConversionStatus.PARTIAL;
        return new ConversionResult(target, status, issues);
    }

    public String targetVersion() {
        return TARGET_VERSION;
    }

    private static EventEnvelope copy(EventEnvelope event) {
        return new EventEnvelope(event.eventId(), event.eventType(), event.version(),
                event.partitionKey(), event.payload().deepCopy(), event.sequence());
    }
}
