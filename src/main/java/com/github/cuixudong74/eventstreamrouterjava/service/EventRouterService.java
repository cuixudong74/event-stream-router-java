package com.github.cuixudong74.eventstreamrouterjava.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.conversion.EventConversionService;
import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamDispatcher;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouterMetrics;
import com.github.cuixudong74.eventstreamrouterjava.model.SubmitResult;
import com.github.cuixudong74.eventstreamrouterjava.model.ValidationException;
import com.github.cuixudong74.eventstreamrouterjava.persistence.FileDurableEventStore;
import com.github.cuixudong74.eventstreamrouterjava.persistence.StoredEvent;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingService;
import com.github.cuixudong74.eventstreamrouterjava.validation.EventValidator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

public class EventRouterService {
    private final ObjectMapper objectMapper;
    private final FileDurableEventStore store;
    private final EventValidator validator;
    private final EventConversionService conversionService;
    private final RoutingService routingService;
    private final DownstreamDispatcher dispatcher;

    public EventRouterService(ObjectMapper objectMapper,
                              FileDurableEventStore store,
                              EventValidator validator,
                              EventConversionService conversionService,
                              RoutingService routingService,
                              DownstreamDispatcher dispatcher) {
        this.objectMapper = objectMapper;
        this.store = store;
        this.validator = validator;
        this.conversionService = conversionService;
        this.routingService = routingService;
        this.dispatcher = dispatcher;
    }

    @PostConstruct
    public void start() {
        dispatcher.start();
    }

    @PreDestroy
    public void stop() {
        dispatcher.stop();
    }

    public SubmitResult submit(JsonNode request) {
        synchronized (store) {
            EventEnvelope original;
            try {
                original = objectMapper.convertValue(request, EventEnvelope.class);
                validator.validate(original);
            } catch (ValidationException | IllegalArgumentException exception) {
                store.recordRejection();
                throw exception instanceof ValidationException validationException
                        ? validationException
                        : new ValidationException(java.util.List.of(
                                new com.github.cuixudong74.eventstreamrouterjava.model.ValidationError(
                                        "$", "MALFORMED_JSON", exception.getMessage())));
            }

            var conversion = conversionService.convert(original);
            long sequence = store.nextSequence();
            EventEnvelope routedEvent = conversion.targetEvent().withSequence(sequence);
            var decision = routingService.route(routedEvent);
            StoredEvent stored = new StoredEvent(sequence, original.withSequence(sequence),
                    routedEvent, decision, conversion.status(), conversion.targetEvent() == null
                            ? java.util.List.of() : conversion.issues());

            boolean queued;
            try {
                queued = dispatcher.prepare(stored);
            } catch (RuntimeException exception) {
                store.recordRejection();
                throw exception;
            }

            try {
                store.append(stored);
            } catch (RuntimeException exception) {
                dispatcher.rollback(stored);
                store.recordRejection();
                throw exception;
            }

            if (!queued) {
                dispatcher.deadLetter(stored, "downstream queue exceeded configured capacity");
            } else {
                dispatcher.publish(stored);
            }

            return new SubmitResult(sequence, original.eventId(), decision.destinationId(),
                    decision.ruleVersion(), conversion.status(), conversion.issues());
        }
    }

    public RouterMetrics metrics() {
        return new RouterMetrics(store.nextSequence(), store.acceptedCount(), store.rejectedCount(),
                store.deadLetters().size(), dispatcher.metrics());
    }
}
