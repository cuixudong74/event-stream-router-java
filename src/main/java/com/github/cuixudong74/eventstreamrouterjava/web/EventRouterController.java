package com.github.cuixudong74.eventstreamrouterjava.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.model.RouterMetrics;
import com.github.cuixudong74.eventstreamrouterjava.model.SubmitResult;
import com.github.cuixudong74.eventstreamrouterjava.service.EventRouterService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class EventRouterController {
    private final EventRouterService service;
    private final ObjectMapper objectMapper;

    public EventRouterController(EventRouterService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/events")
    @ResponseStatus(HttpStatus.CREATED)
    public SubmitResult submit(@RequestBody SubmitEventRequest request) {
        return service.submit(objectMapper.valueToTree(request));
    }

    @GetMapping("/metrics")
    public RouterMetrics metrics() {
        return service.metrics();
    }
}
