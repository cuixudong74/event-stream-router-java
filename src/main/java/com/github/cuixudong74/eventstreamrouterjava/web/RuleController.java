package com.github.cuixudong74.eventstreamrouterjava.web;

import com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion;
import com.github.cuixudong74.eventstreamrouterjava.service.RuleAdminService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/rules")
public class RuleController {
    private final RuleAdminService service;

    public RuleController(RuleAdminService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public void publish(@RequestBody RuleVersion ruleVersion) {
        service.publish(ruleVersion);
    }

    @GetMapping
    public List<RuleVersion> list() {
        return service.list();
    }
}
