package com.github.cuixudong74.eventstreamrouterjava.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.model.RuleVersion;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class RuleCatalog {
    private final CopyOnWriteArrayList<RuleVersion> versions = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Path file;

    public RuleCatalog(Iterable<RuleVersion> initialVersions) {
        this(initialVersions, null);
    }

    public RuleCatalog(Iterable<RuleVersion> initialVersions, Path file) {
        this.file = file;
        loadIfPresent();
        for (RuleVersion version : initialVersions) {
            if (versions.stream().noneMatch(existing -> existing.ruleVersion().equals(version.ruleVersion()))) {
                publish(version);
            }
        }
    }

    public synchronized void publish(RuleVersion ruleVersion) {
        if (ruleVersion == null || ruleVersion.ruleVersion() == null || ruleVersion.ruleVersion().isBlank()) {
            throw new IllegalArgumentException("rule version is required");
        }
        if (ruleVersion.startsAtSequence() <= 0) {
            throw new IllegalArgumentException("rule start sequence must be positive");
        }
        if (versions.stream().anyMatch(v -> v.ruleVersion().equals(ruleVersion.ruleVersion()))) {
            throw new IllegalArgumentException("rule version already exists");
        }

        List<RuleVersion> copy = new ArrayList<>(versions);
        copy.add(ruleVersion);
        copy.sort(Comparator.comparingLong(RuleVersion::startsAtSequence));
        RuleVersion previous = null;
        for (RuleVersion version : copy) {
            if (previous != null && version.startsAtSequence() <= previous.startsAtSequence()) {
                throw new IllegalArgumentException("rule versions must have distinct increasing start sequences");
            }
            previous = version;
        }
        appendIfConfigured(ruleVersion);
        versions.clear();
        versions.addAll(copy);
    }

    public Optional<RuleVersion> effectiveAt(long sequence) {
        RuleVersion result = null;
        for (RuleVersion version : versions) {
            if (version.startsAtSequence() <= sequence) {
                result = version;
            } else {
                break;
            }
        }
        return Optional.ofNullable(result);
    }

    public List<RuleVersion> all() {
        return List.copyOf(versions);
    }

    private void loadIfPresent() {
        if (file == null || !Files.exists(file)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    versions.add(objectMapper.readValue(line, RuleVersion.class));
                }
            }
            List<RuleVersion> copy = new ArrayList<>(versions);
            copy.sort(Comparator.comparingLong(RuleVersion::startsAtSequence));
            versions.clear();
            versions.addAll(copy);
        } catch (IOException e) {
            throw new IllegalStateException("failed to load rule catalog", e);
        }
    }

    private void appendIfConfigured(RuleVersion ruleVersion) {
        if (file == null) {
            return;
        }
        try {
            Files.createDirectories(file.getParent());
            String line = objectMapper.writeValueAsString(ruleVersion) + System.lineSeparator();
            Files.write(file, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist rule version", e);
        }
    }
}
