package com.github.cuixudong74.eventstreamrouterjava.model;

import java.util.List;

public class ValidationException extends RuntimeException {
    private final transient List<ValidationError> errors;

    public ValidationException(List<ValidationError> errors) {
        super("event validation failed");
        this.errors = List.copyOf(errors);
    }

    public List<ValidationError> getErrors() {
        return errors;
    }
}
