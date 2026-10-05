package com.github.cuixudong74.eventstreamrouterjava.api;

import com.github.cuixudong74.eventstreamrouterjava.service.IngestException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 统一把拒绝原因以结构化方式返回，不静默丢弃。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(IngestException.class)
    public ResponseEntity<Map<String, Object>> handleIngest(IngestException e) {
        return ResponseEntity.status(e.status())
                .body(Map.of("error", "INGEST_REJECTED", "message", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", "BAD_REQUEST", "message", e.getMessage()));
    }
}
