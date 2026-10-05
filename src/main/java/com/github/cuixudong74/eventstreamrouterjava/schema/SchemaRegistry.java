package com.github.cuixudong74.eventstreamrouterjava.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 已注册结构版本的仓库，支持持久化与运行中注册新版本。
 * 版本一经注册不可变，保证历史事件可重复校验与转换。
 */
@Component
public class SchemaRegistry {

    private final Map<Integer, EventSchema> schemas = new ConcurrentHashMap<>();
    private final Path storeFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public SchemaRegistry(RouterProperties properties) {
        this.storeFile = Path.of(properties.getDataDir(), "schemas.json");
        load();
    }

    public synchronized void register(EventSchema schema) {
        EventSchema existing = schemas.get(schema.version());
        if (existing != null && !existing.equals(schema)) {
            throw new IllegalArgumentException(
                    "schema version " + schema.version() + " already registered with different definition");
        }
        schemas.put(schema.version(), schema);
        persist();
    }

    public Optional<EventSchema> find(int version) {
        return Optional.ofNullable(schemas.get(version));
    }

    public Set<Integer> supportedVersions() {
        return new TreeSet<>(schemas.keySet());
    }

    private void load() {
        if (!Files.isRegularFile(storeFile)) {
            return;
        }
        try {
            EventSchema[] loaded = mapper.readValue(storeFile.toFile(), EventSchema[].class);
            for (EventSchema s : loaded) {
                schemas.put(s.version(), s);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to load schemas from " + storeFile, e);
        }
    }

    private void persist() {
        try {
            Files.createDirectories(storeFile.getParent());
            List<EventSchema> all = schemas.values().stream()
                    .sorted((a, b) -> Integer.compare(a.version(), b.version()))
                    .toList();
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), all);
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist schemas to " + storeFile, e);
        }
    }
}
