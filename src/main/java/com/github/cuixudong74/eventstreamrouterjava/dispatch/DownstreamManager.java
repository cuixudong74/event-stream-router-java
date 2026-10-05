package com.github.cuixudong74.eventstreamrouterjava.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import com.github.cuixudong74.eventstreamrouterjava.journal.EventJournal;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 下游注册中心：运行中动态增删下游。新增下游从明确起点（当前日志头部或显式位点）开始；
 * 删除只停止自身投递，不影响其他下游；已有下游的检查点不因变更被重置。
 * 注册表持久化，重启后自动恢复各下游投递循环。
 */
@Component
public class DownstreamManager {

    private final Map<String, DownstreamSink> sinks = new ConcurrentHashMap<>();
    private final Map<String, Downstream> downstreams = new ConcurrentHashMap<>();
    private final Map<String, DownstreamDispatcher> dispatchers = new ConcurrentHashMap<>();
    private final EventJournal journal;
    private final CheckpointStore checkpoints;
    private final RouterProperties properties;
    private final Path storeFile;
    private final ObjectMapper mapper = new ObjectMapper();

    public DownstreamManager(EventJournal journal, CheckpointStore checkpoints, RouterProperties properties) {
        this.journal = journal;
        this.checkpoints = checkpoints;
        this.properties = properties;
        this.storeFile = Path.of(properties.getDataDir(), "downstreams.json");
    }

    @PostConstruct
    public void init() {
        if (Files.isRegularFile(storeFile)) {
            try {
                Downstream[] loaded = mapper.readValue(storeFile.toFile(), Downstream[].class);
                for (Downstream d : loaded) {
                    downstreams.put(d.id(), d);
                }
            } catch (IOException e) {
                throw new IllegalStateException("failed to load downstreams from " + storeFile, e);
            }
        }
        // 重启恢复：为每个已注册下游重启投递循环，从各自检查点继续，不跳过事件。
        for (Downstream d : downstreams.values()) {
            startDispatcher(d);
        }
    }

    @PreDestroy
    public void shutdown() {
        dispatchers.values().forEach(DownstreamDispatcher::stop);
    }

    /** 注册下游消费方实现（如 HTTP、日志、测试录制 sink）。 */
    public void registerSink(String name, DownstreamSink sink) {
        sinks.put(name, sink);
    }

    /** 注册下游，从当前日志头部（或显式 fromSeq）开始消费。 */
    public synchronized Downstream addDownstream(String id, String sinkName, Optional<Long> fromSeq) {
        if (downstreams.containsKey(id)) {
            throw new IllegalArgumentException("downstream '" + id + "' already exists");
        }
        if (!sinks.containsKey(sinkName)) {
            throw new IllegalArgumentException("unknown sink '" + sinkName + "'");
        }
        long startSeq = fromSeq.orElse(journal.lastSeq() + 1);
        Downstream d = new Downstream(id, sinkName, startSeq, System.currentTimeMillis());
        downstreams.put(id, d);
        persist();
        startDispatcher(d);
        return d;
    }

    /** 删除下游：停止其投递循环，保留检查点，不影响其他下游。 */
    public synchronized boolean removeDownstream(String id) {
        Downstream removed = downstreams.remove(id);
        if (removed == null) {
            return false;
        }
        DownstreamDispatcher dispatcher = dispatchers.remove(id);
        if (dispatcher != null) {
            dispatcher.stop();
        }
        persist();
        return true;
    }

    public List<Downstream> list() {
        return downstreams.values().stream()
                .sorted((a, b) -> a.id().compareTo(b.id()))
                .toList();
    }

    /** 各下游滞后量快照，用于可观测性与背压判定。 */
    public Map<String, Long> lagSnapshot() {
        Map<String, Long> lag = new LinkedHashMap<>();
        dispatchers.forEach((id, d) -> lag.put(id, d.lag()));
        return lag;
    }

    /** 将下游回退到指定位点重放，结果与首次投递一致（日志不可变）。 */
    public synchronized void replay(String id, long fromSeq) {
        DownstreamDispatcher dispatcher = dispatchers.get(id);
        if (dispatcher == null) {
            throw new IllegalArgumentException("unknown downstream '" + id + "'");
        }
        dispatcher.stop();
        dispatcher.replay(fromSeq);
        dispatcher.start();
    }

    private void startDispatcher(Downstream d) {
        DownstreamSink sink = sinks.get(d.sinkName());
        if (sink == null) {
            // sink 尚未注册（如测试后注册）：延迟到 registerSink 后由调用方触发。
            return;
        }
        DownstreamDispatcher dispatcher = new DownstreamDispatcher(
                d.id(), sink, journal, checkpoints, d.startSeq(), properties.getRetryBackoffMillis());
        dispatchers.put(d.id(), dispatcher);
        dispatcher.start();
    }

    private void persist() {
        try {
            Files.createDirectories(storeFile.getParent());
            Path tmp = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            mapper.writeValue(tmp.toFile(), new ArrayList<>(downstreams.values()));
            Files.move(tmp, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist downstreams to " + storeFile, e);
        }
    }
}
