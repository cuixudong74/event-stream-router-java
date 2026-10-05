package com.github.cuixudong74.eventstreamrouterjava.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.cuixudong74.eventstreamrouterjava.model.ValidationError;

import java.util.List;

public interface EventSchema {
    String version();

    void validatePayload(JsonNode payload, List<ValidationError> errors);
}
