package com.github.cuixudong74.eventstreamrouterjava.service;

import com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion;
import com.github.cuixudong74.eventstreamrouterjava.persistence.FileDurableEventStore;
import com.github.cuixudong74.eventstreamrouterjava.routing.RuleCatalog;

import java.util.List;

public class RuleAdminService {
    private final RuleCatalog ruleCatalog;
    private final FileDurableEventStore store;

    public RuleAdminService(RuleCatalog ruleCatalog, FileDurableEventStore store) {
        this.ruleCatalog = ruleCatalog;
        this.store = store;
    }

    public void publish(RuleVersion ruleVersion) {
        synchronized (store) {
            long currentNext = store.nextSequence();
            if (ruleVersion.startsAtSequence() < currentNext) {
                throw new IllegalArgumentException("new rules cannot start at or before an existing sequence");
            }
            ruleCatalog.publish(ruleVersion);
        }
    }

    public List<RuleVersion> list() {
        return ruleCatalog.all();
    }
}
