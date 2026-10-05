package com.github.cuixudong74.eventstreamrouterjava.service;

public class BackpressureException extends RuntimeException {
    public BackpressureException(String message) {
        super(message);
    }
}
