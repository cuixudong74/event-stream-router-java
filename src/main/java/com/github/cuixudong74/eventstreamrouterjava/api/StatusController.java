package com.github.cuixudong74.eventstreamrouterjava.api;

import com.github.cuixudong74.eventstreamrouterjava.service.RouterStatusService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 状态与可观测性端点：已提交位点、各下游检查点与滞后。
 */
@RestController
public class StatusController {

    private final RouterStatusService statusService;

    public StatusController(RouterStatusService statusService) {
        this.statusService = statusService;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return statusService.snapshot();
    }
}
