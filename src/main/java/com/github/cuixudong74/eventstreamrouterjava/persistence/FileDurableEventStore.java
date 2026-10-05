package com.github.cuixudong74.eventstreamrouterjava.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.cuixudong74.eventstreamrouterjava.model.DeadLetterRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class FileDurableEventStore implements DurableEventStore {
    private static final String EVENTS_FILE = "events.log";
    private static final String STATE_FILE = "state.json";
    private static final String DEAD_LETTERS_FILE = "dead-letters.log";

    private final ObjectMapper objectMapper;
    private final Path directory;
    private final Path eventsPath;
    private final Path statePath;
    private final Path deadLettersPath;
    private final Object lock = new Object();

    private final TreeMap<Long, StoredEvent> events = new TreeMap<>();
    private final TreeMap<String, DownstreamProgress> downstreams = new TreeMap<>();
    private final List<DeadLetterRecord> deadLetters = new ArrayList<>();
    private long nextSequence = 1;
    private long acceptedCount;
    private long rejectedCount;

    public FileDurableEventStore(ObjectMapper objectMapper, Path directory) {
        this.objectMapper = objectMapper.copy().registerModule(new JavaTimeModule());
        this.directory = directory;
        this.eventsPath = directory.resolve(EVENTS_FILE);
        this.statePath = directory.resolve(STATE_FILE);
        this.deadLettersPath = directory.resolve(DEAD_LETTERS_FILE);
        load();
    }

    @Override
    public StoredEvent append(StoredEvent event) {
        synchronized (lock) {
            if (event.sequence() != nextSequence) {
                throw new IllegalStateException("expected sequence " + nextSequence + " but got " + event.sequence());
            }
            appendJsonLine(eventsPath, event);
            events.put(event.sequence(), event);
            nextSequence++;
            acceptedCount++;
            saveState();
            return event;
        }
    }

    @Override
    public Optional<StoredEvent> read(long sequence) {
        synchronized (lock) {
            return Optional.ofNullable(events.get(sequence));
        }
    }

    @Override
    public List<StoredEvent> range(long fromSequenceInclusive, long toSequenceExclusive) {
        synchronized (lock) {
            if (fromSequenceInclusive >= toSequenceExclusive) {
                return List.of();
            }
            return List.copyOf(events.subMap(fromSequenceInclusive, true,
                    toSequenceExclusive - 1, true).values());
        }
    }

    @Override
    public long nextSequence() {
        synchronized (lock) {
            return nextSequence;
        }
    }

    @Override
    public List<StoredEvent> all() {
        synchronized (lock) {
            return List.copyOf(events.values());
        }
    }

    @Override
    public void commit(String downstreamId, long sequence, boolean delivered) {
        synchronized (lock) {
            DownstreamProgress current = downstreams.get(downstreamId);
            if (current == null) {
                throw new IllegalArgumentException("unknown downstream: " + downstreamId);
            }
            if (sequence <= current.committedSequence()) {
                return;
            }
            downstreams.put(downstreamId, new DownstreamProgress(
                    current.downstreamId(), current.startSequence(), sequence,
                    current.queueCapacity(), current.backpressurePolicy(), current.state(),
                    current.deliveredCount() + (delivered ? 1 : 0),
                    current.deadLetterCount()));
            saveState();
        }
    }

    @Override
    public DownstreamProgress progress(String downstreamId) {
        synchronized (lock) {
            DownstreamProgress progress = downstreams.get(downstreamId);
            if (progress == null) {
                throw new IllegalArgumentException("unknown downstream: " + downstreamId);
            }
            return progress;
        }
    }

    @Override
    public List<DownstreamProgress> progressList() {
        synchronized (lock) {
            return List.copyOf(downstreams.values());
        }
    }

    @Override
    public void addDownstream(DownstreamProgress progress) {
        synchronized (lock) {
            if (downstreams.putIfAbsent(progress.downstreamId(), progress) != null) {
                throw new IllegalArgumentException("downstream already exists: " + progress.downstreamId());
            }
            saveState();
        }
    }

    @Override
    public void updateProgress(DownstreamProgress progress) {
        synchronized (lock) {
            if (!downstreams.containsKey(progress.downstreamId())) {
                throw new IllegalArgumentException("unknown downstream: " + progress.downstreamId());
            }
            downstreams.put(progress.downstreamId(), progress);
            saveState();
        }
    }

    @Override
    public void addDeadLetter(DeadLetterRecord record) {
        synchronized (lock) {
            appendJsonLine(deadLettersPath, record);
            deadLetters.add(record);
            DownstreamProgress current = downstreams.get(record.downstreamId());
            if (current != null) {
                downstreams.put(record.downstreamId(), new DownstreamProgress(
                        current.downstreamId(), current.startSequence(), current.committedSequence(),
                        current.queueCapacity(), current.backpressurePolicy(), current.state(),
                        current.deliveredCount(), current.deadLetterCount() + 1));
            }
            saveState();
        }
    }

    @Override
    public List<DeadLetterRecord> deadLetters() {
        synchronized (lock) {
            return List.copyOf(deadLetters);
        }
    }

    public long acceptedCount() {
        synchronized (lock) {
            return acceptedCount;
        }
    }

    public long rejectedCount() {
        synchronized (lock) {
            return rejectedCount;
        }
    }

    public void recordRejection() {
        synchronized (lock) {
            rejectedCount++;
            saveState();
        }
    }

    private void load() {
        try {
            Files.createDirectories(directory);
            loadEvents();
            loadState();
            loadDeadLetters();
        } catch (IOException e) {
            throw new IllegalStateException("failed to load durable event store", e);
        }
    }

    private void loadEvents() throws IOException {
        if (!Files.exists(eventsPath)) {
            return;
        }
        for (String line : Files.readAllLines(eventsPath, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                StoredEvent event = readJson(line, StoredEvent.class);
                events.put(event.sequence(), event);
            }
        }
        nextSequence = events.isEmpty() ? 1 : events.lastKey() + 1;
    }

    private void loadState() throws IOException {
        if (!Files.exists(statePath)) {
            return;
        }
        StateSnapshot snapshot = readJson(Files.readString(statePath, StandardCharsets.UTF_8), StateSnapshot.class);
        acceptedCount = snapshot.acceptedCount();
        rejectedCount = snapshot.rejectedCount();
        for (DownstreamProgress progress : snapshot.downstreams()) {
            downstreams.put(progress.downstreamId(), progress);
        }
    }

    private void loadDeadLetters() throws IOException {
        if (!Files.exists(deadLettersPath)) {
            return;
        }
        for (String line : Files.readAllLines(deadLettersPath, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                deadLetters.add(readJson(line, DeadLetterRecord.class));
            }
        }
    }

    private void saveState() {
        try {
            StateSnapshot snapshot = new StateSnapshot(nextSequence, acceptedCount, rejectedCount,
                    List.copyOf(downstreams.values()));
            Path temp = directory.resolve(STATE_FILE + ".tmp");
            Files.writeString(temp, objectMapper.writeValueAsString(snapshot) + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(temp, statePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new IllegalStateException("failed to save durable state", e);
        }
    }

    private void appendJsonLine(Path path, Object value) {
        try {
            String line = objectMapper.writeValueAsString(value) + System.lineSeparator();
            Files.write(path, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("failed to append durable record", e);
        }
    }

    private <T> T readJson(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (IOException e) {
            throw new IllegalStateException("failed to read durable record", e);
        }
    }

    private record StateSnapshot(
            long nextSequence,
            long acceptedCount,
            long rejectedCount,
            List<DownstreamProgress> downstreams
    ) {
    }
}
