package com.github.cuixudong74.eventstreamrouterjava.web;

import com.github.cuixudong74.eventstreamrouterjava.model.ValidationError;
import com.github.cuixudong74.eventstreamrouterjava.model.ValidationException;
import com.github.cuixudong74.eventstreamrouterjava.service.BackpressureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ApiError> handleValidation(ValidationException exception) {
        return ResponseEntity.unprocessableEntity().body(new ApiError(
                "VALIDATION_FAILED",
                exception.getMessage(),
                exception.getErrors().stream().map(GlobalExceptionHandler::fieldError).toList()));
    }

    @ExceptionHandler(BackpressureException.class)
    public ResponseEntity<ApiError> handleBackpressure(BackpressureException exception) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiError("BACKPRESSURE_LIMIT", exception.getMessage(), List.of()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleBadRequest(IllegalArgumentException exception) {
        return ResponseEntity.badRequest().body(new ApiError("BAD_REQUEST", exception.getMessage(), List.of()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ApiError> handleConflict(IllegalStateException exception) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ApiError("ILLEGAL_STATE",
                exception.getMessage(), List.of()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handle(Exception exception) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiError("INTERNAL_ERROR", exception.getMessage(), List.of()));
    }

    private static ApiError.FieldError fieldError(ValidationError error) {
        return new ApiError.FieldError(error.field(), error.code(), error.reason());
    }
}
