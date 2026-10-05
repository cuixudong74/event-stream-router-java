package com.github.cuixudong74.eventstreamrouterjava.dispatch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 持久化每个下游已确认投递的位点（checkpoint）。重启后据此恢复：
 * checkpoint 之前的视为已投递，之后的重新投递（可能重复，下游按 eventId 去重）。
 */
@Component
public class CheckpointStore {

    private final Map<String, Long> checkpoints = new ConcurrentHashMap<>();
    private final Path storeFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public CheckpointStore(RouterProperties properties) {
        this.storeFile = Path.of(properties.getDataDir(), "checkpoints.json");
        load();
    }

    public OptionalLong checkpoint(String downstreamId) {
        Long v = checkpoints.get(downstreamId);
        return v == null ? OptionalLong.empty() : OptionalLong.of(v);
    }

    public synchronized void save(String downstreamId, long seq) {
        checkpoints.merge(downstreamId, seq, Math::max);
        persist();
    }

    /** 无条件设置检查点（用于重放回退）。 */
    public synchronized void reset(String downstreamId, long seq) {
        checkpoints.put(downstreamId, seq);
        persist();
    }

    public Map<String, Long> all() {
        return Map.copyOf(checkpoints);
    }

    private void load() {
        if (!Files.isRegularFile(storeFile)) {
            return;
        }
        try {
            Map<String, Long> loaded = mapper.readValue(storeFile.toFile(), new TypeReference<>() {});
            checkpoints.putAll(loaded);
        } catch (IOException e) {
            throw new IllegalStateException("failed to load checkpoints from " + storeFile, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(storeFile.getParent());
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), new HashMap<>(checkpoints));
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist checkpoints to " + storeFile, e);
        }
    }
}
