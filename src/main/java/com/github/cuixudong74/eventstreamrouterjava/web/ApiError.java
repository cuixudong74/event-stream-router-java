package com.github.cuixudong74.eventstreamrouterjava.web;

import java.util.List;

public record ApiError(String code, String message, List<FieldError> errors) {
    public record FieldError(String field, String code, String reason) {
    }
}
