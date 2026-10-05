package com.github.cuixudong74.eventstreamrouterjava.model;

public record ValidationError(String field, String code, String reason) {
}
