package com.github.cuixudong74.eventstreamrouterjava.tests.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.cuixudong74.eventstreamrouterjava.conversion.ConversionRegistry;
import com.github.cuixudong74.eventstreamrouterjava.conversion.EventConversionService;
import com.github.cuixudong74.eventstreamrouterjava.conversion.V1ToV2EventConverter;
import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamDispatcher;
import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamReceiver;
import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamRegistry;
import com.github.cuixudong74.eventstreamrouterjava.model.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.model.RouteDecision;
import com.github.cuixudong74.eventstreamrouterjava.persistence.FileDurableEventStore;
import com.github.cuixudong74.eventstreamrouterjava.routing.RuleCatalog;
import com.github.cuixudong74.eventstreamrouterjava.routing.RuleEvaluator;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingService;
import com.github.cuixudong74.eventstreamrouterjava.service.DownstreamService;
import com.github.cuixudong74.eventstreamrouterjava.service.EventRouterService;
import com.github.cuixudong74.eventstreamrouterjava.validation.EventValidator;
import com.github.cuixudong74.eventstreamrouterjava.validation.SchemaRegistry;

import java.nio.file.Path;
import java.util.List;

public final class RouterTestHarness implements AutoCloseable {
    public final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    public final FileDurableEventStore store;
    public final DownstreamRegistry registry = new DownstreamRegistry();
    public final DownstreamDispatcher dispatcher;
    public final EventRouterService router;
    public final DownstreamService downstreams;
    public final RuleCatalog rules;
    public final Path directory;

    private RouterTestHarness(Path directory, List<com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion> initialRules) {
        this.directory = directory;
        this.store = new FileDurableEventStore(objectMapper, directory);
        this.dispatcher = new DownstreamDispatcher(store, registry);
        var schemaRegistry = new SchemaRegistry();
        var conversionRegistry = new ConversionRegistry(List.of(new V1ToV2EventConverter()));
        var conversionService = new EventConversionService(conversionRegistry);
        this.rules = new RuleCatalog(initialRules, directory.resolve("rules.log"));
        var routing = new RoutingService(rules, new RuleEvaluator());
        this.router = new EventRouterService(objectMapper, store, new EventValidator(schemaRegistry),
                conversionService, routing, dispatcher);
        this.downstreams = new DownstreamService(store, dispatcher);
    }

    public static RouterTestHarness create(Path directory) {
        var defaultRoute = new com.github.cuixudong74.eventstreamrouterjava.model.RouteDefinition(
                "all-orders", "order.created", java.util.Map.of(), "orders");
        var payments = new com.github.cuixudong74.eventstreamrouterjava.model.RouteDefinition(
                "business-to-payments", "order.created",
                java.util.Map.of("customerType", "BUSINESS"), "payments");
        var rules = List.of(new com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion(
                "rules-v1", 1, List.of(payments, defaultRoute), "orders"));
        RouterTestHarness harness = new RouterTestHarness(directory, rules);
        harness.dispatcher.start();
        return harness;
    }

    public static RouterTestHarness reopen(Path directory) {
        RouterTestHarness harness = new RouterTestHarness(directory, List.of());
        harness.dispatcher.start();
        return harness;
    }

    public void register(String destination, DownstreamReceiver receiver) {
        registry.register(receiver);
    }

    public static DownstreamReceiver receiver(String destination, java.util.List<EventEnvelope> sink) {
        return new DownstreamReceiver() {
            @Override
            public String destinationId() {
                return destination;
            }

            @Override
            public void receive(EventEnvelope event, RouteDecision decision) {
                sink.add(event);
            }
        };
    }

    @Override
    public void close() {
        dispatcher.stop();
    }
}
