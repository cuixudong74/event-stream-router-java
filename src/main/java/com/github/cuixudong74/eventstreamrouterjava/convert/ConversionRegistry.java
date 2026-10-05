package com.github.cuixudong74.eventstreamrouterjava.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 维护按版本配置的转换规则（相邻版本一跳），可解析 fromVersion -> targetVersion 的转换链。
 * 规则持久化到磁盘，重启后转换行为不变，保证同一事件转换结果一致。
 */
@Component
public class ConversionRegistry {

    private final Map<Integer, VersionConversion> hopsByFromVersion = new ConcurrentHashMap<>();
    private final Path storeFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public ConversionRegistry(RouterProperties properties) {
        this.storeFile = Path.of(properties.getDataDir(), "conversions.json");
        load();
    }

    public synchronized void register(VersionConversion conversion) {
        hopsByFromVersion.put(conversion.fromVersion(), conversion);
        persist();
    }

    /**
     * 返回从 from 到 target 的逐跳转换链；from == target 返回空链；无法到达时返回空。
     */
    public Optional<List<VersionConversion>> resolveChain(int fromVersion, int targetVersion) {
        List<VersionConversion> chain = new ArrayList<>();
        int current = fromVersion;
        int guard = 0;
        while (current != targetVersion) {
            if (++guard > 1000) {
                return Optional.empty();
            }
            VersionConversion hop = hopsByFromVersion.get(current);
            if (hop == null) {
                return Optional.empty();
            }
            chain.add(hop);
            current = hop.toVersion();
        }
        return Optional.of(chain);
    }

    private void load() {
        if (!Files.isRegularFile(storeFile)) {
            return;
        }
        try {
            VersionConversion[] loaded = mapper.readValue(storeFile.toFile(), VersionConversion[].class);
            for (VersionConversion c : loaded) {
                hopsByFromVersion.put(c.fromVersion(), c);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to load conversions from " + storeFile, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(storeFile.getParent());
            List<VersionConversion> all = hopsByFromVersion.values().stream()
                    .sorted((a, b) -> Integer.compare(a.fromVersion(), b.fromVersion()))
                    .toList();
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), all);
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist conversions to " + storeFile, e);
        }
    }
}
