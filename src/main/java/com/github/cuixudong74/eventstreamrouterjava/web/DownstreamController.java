package com.github.cuixudong74.eventstreamrouterjava.web;

import com.github.cuixudong74.eventstreamrouterjava.model.DownstreamMetrics;
import com.github.cuixudong74.eventstreamrouterjava.service.DownstreamService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/downstreams")
public class DownstreamController {
    private final DownstreamService service;

    public DownstreamController(DownstreamService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public void add(@RequestBody AddDownstreamRequest request) {
        service.add(request.id(), request.startMode(), request.queueCapacity(), request.policy());
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void remove(@PathVariable String id) throws InterruptedException {
        service.remove(id);
    }

    @PostMapping("/{id}/replay")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void replay(@PathVariable String id, @RequestParam long fromSequence) {
        service.replay(id, fromSequence);
    }

    @GetMapping
    public List<DownstreamMetrics> metrics() {
        return service.metrics();
    }

    public record AddDownstreamRequest(
            String id,
            String startMode,
            Integer queueCapacity,
            com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy policy
    ) {
    }
}
