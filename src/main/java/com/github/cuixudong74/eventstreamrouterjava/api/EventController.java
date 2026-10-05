package com.github.cuixudong74.eventstreamrouterjava.api;

import com.github.cuixudong74.eventstreamrouterjava.core.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.service.IngestResult;
import com.github.cuixudong74.eventstreamrouterjava.service.IngestService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 事件接入端点：校验通过即提交并返回分配的位点；失败返回 4xx 与逐字段原因。
 */
@RestController
public class EventController {

    private final IngestService ingestService;

    public EventController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping("/events")
    public ResponseEntity<IngestResult> submit(@RequestBody EventEnvelope envelope) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(ingestService.ingest(envelope));
    }
}
