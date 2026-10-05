package com.github.cuixudong74.eventstreamrouterjava.service;

/**
 * 接入被拒绝：携带 HTTP 状态与可解释原因。
 */
public class IngestException extends RuntimeException {

    private final int status;

    public IngestException(int status, String message) {
        super(message);
        this.status = status;
    }

    public int status() { return status; }
}
