package com.github.cuixudong74.eventstreamrouterjava.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 版本化路由表。规则变更在指定位点 effectiveFromSeq 生效：
 * 位点两侧的事件各自按其生效时的规则版本路由，任一事件只被一个规则版本评估一次。
 * 版本列表持久化，重启后位点-版本对应关系不变。
 */
@Component
public class RoutingTable {

    private final List<RuleSetVersion> versions = new CopyOnWriteArrayList<>();
    private final Path storeFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public RoutingTable(RouterProperties properties) {
        this.storeFile = Path.of(properties.getDataDir(), "rulesets.json");
        load();
    }

    /** 激活新规则集，从 fromSeq（含）生效。返回新版本。 */
    public synchronized RuleSetVersion activate(List<RoutingRule> rules, long fromSeq) {
        if (!versions.isEmpty()) {
            long lastFrom = versions.get(versions.size() - 1).effectiveFromSeq();
            if (fromSeq < lastFrom) {
                throw new IllegalArgumentException(
                        "effectiveFromSeq " + fromSeq + " is before current version start " + lastFrom);
            }
        }
        List<RoutingRule> sorted = new ArrayList<>(rules);
        sorted.sort(Comparator.comparingInt(RoutingRule::priority));
        long version = versions.isEmpty() ? 1 : versions.get(versions.size() - 1).version() + 1;
        RuleSetVersion v = new RuleSetVersion(version, fromSeq, List.copyOf(sorted), System.currentTimeMillis());
        versions.add(v);
        persist();
        return v;
    }

    /** 找到对给定 seq 生效的规则版本（effectiveFromSeq <= seq 的最后一个版本）。 */
    public Optional<RuleSetVersion> versionFor(long seq) {
        RuleSetVersion found = null;
        for (RuleSetVersion v : versions) {
            if (v.effectiveFromSeq() <= seq) {
                found = v;
            } else {
                break;
            }
        }
        return Optional.ofNullable(found);
    }

    public List<RuleSetVersion> versions() {
        return List.copyOf(versions);
    }

    private void load() {
        if (!Files.isRegularFile(storeFile)) {
            return;
        }
        try {
            RuleSetVersion[] loaded = mapper.readValue(storeFile.toFile(), RuleSetVersion[].class);
            versions.addAll(List.of(loaded));
        } catch (IOException e) {
            throw new IllegalStateException("failed to load rulesets from " + storeFile, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(storeFile.getParent());
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), new ArrayList<>(versions));
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist rulesets to " + storeFile, e);
        }
    }
}
