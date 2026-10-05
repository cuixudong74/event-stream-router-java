package com.github.cuixudong74.eventstreamrouterjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.conversion.ConversionRegistry;
import com.github.cuixudong74.eventstreamrouterjava.conversion.EventConversionService;
import com.github.cuixudong74.eventstreamrouterjava.conversion.V1ToV2EventConverter;
import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamDispatcher;
import com.github.cuixudong74.eventstreamrouterjava.downstream.DownstreamRegistry;
import com.github.cuixudong74.eventstreamrouterjava.persistence.FileDurableEventStore;
import com.github.cuixudong74.eventstreamrouterjava.routing.RuleCatalog;
import com.github.cuixudong74.eventstreamrouterjava.routing.RuleEvaluator;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingService;
import com.github.cuixudong74.eventstreamrouterjava.service.DownstreamService;
import com.github.cuixudong74.eventstreamrouterjava.service.EventRouterService;
import com.github.cuixudong74.eventstreamrouterjava.validation.EventValidator;
import com.github.cuixudong74.eventstreamrouterjava.validation.SchemaRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

@Configuration
public class RouterConfiguration {
    @Bean(destroyMethod = "stop")
    public DownstreamDispatcher downstreamDispatcher(FileDurableEventStore store,
                                                     DownstreamRegistry downstreamRegistry) {
        DownstreamDispatcher dispatcher = new DownstreamDispatcher(store, downstreamRegistry);
        dispatcher.start();
        return dispatcher;
    }

    @Bean
    public FileDurableEventStore fileDurableEventStore(ObjectMapper objectMapper,
                                                       @Value("${router.data-directory:./data/router}") String dataDirectory) {
        return new FileDurableEventStore(objectMapper, Path.of(dataDirectory));
    }

    @Bean
    public SchemaRegistry schemaRegistry() {
        return new SchemaRegistry();
    }

    @Bean
    public EventValidator eventValidator(SchemaRegistry schemaRegistry) {
        return new EventValidator(schemaRegistry);
    }

    @Bean
    public ConversionRegistry conversionRegistry() {
        return new ConversionRegistry(List.of(new V1ToV2EventConverter()));
    }

    @Bean
    public EventConversionService eventConversionService(ConversionRegistry conversionRegistry) {
        return new EventConversionService(conversionRegistry);
    }

    @Bean
    public RuleCatalog ruleCatalog(@Value("${router.data-directory:./data/router}") String dataDirectory) {
        var routes = List.of(
                new com.github.cuixudong74.eventstreamrouterjava.model.RouteDefinition(
                        "orders-to-payments", "order.created",
                        java.util.Map.of("customerType", "BUSINESS"), "payments"),
                new com.github.cuixudong74.eventstreamrouterjava.model.RouteDefinition(
                        "orders-default", "order.created", java.util.Map.of(), "orders"));
        var initial = new com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion(
                "rules-v1", 1, routes, "orders");
        return new RuleCatalog(List.of(initial), Path.of(dataDirectory).resolve("rules.log"));
    }

    @Bean
    public RuleEvaluator ruleEvaluator() {
        return new RuleEvaluator();
    }

    @Bean
    public RoutingService routingService(RuleCatalog ruleCatalog, RuleEvaluator ruleEvaluator) {
        return new RoutingService(ruleCatalog, ruleEvaluator);
    }

    @Bean
    public DownstreamRegistry downstreamRegistry() {
        return new DownstreamRegistry();
    }

    @Bean
    public EventRouterService eventRouterService(ObjectMapper objectMapper,
                                                 FileDurableEventStore store,
                                                 EventValidator eventValidator,
                                                 EventConversionService eventConversionService,
                                                 RoutingService routingService,
                                                 DownstreamDispatcher downstreamDispatcher) {
        return new EventRouterService(objectMapper, store, eventValidator,
                eventConversionService, routingService, downstreamDispatcher);
    }

    @Bean
    public DownstreamService downstreamService(FileDurableEventStore store,
                                               DownstreamDispatcher downstreamDispatcher) {
        return new DownstreamService(store, downstreamDispatcher);
    }

    @Bean
    public com.github.cuixudong74.eventstreamrouterjava.service.RuleAdminService ruleAdminService(
            RuleCatalog ruleCatalog, FileDurableEventStore store) {
        return new com.github.cuixudong74.eventstreamrouterjava.service.RuleAdminService(ruleCatalog, store);
    }
}
