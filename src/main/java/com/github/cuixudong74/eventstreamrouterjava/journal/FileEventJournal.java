package com.github.cuixudong74.eventstreamrouterjava.journal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.cuixudong74.eventstreamrouterjava.core.JournalEntry;
import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于本地文件的 JSONL 只追加日志。写入即落盘（flush），重启后完整重放恢复，
 * 是恢复与下游重放的唯一事实来源。
 * 注意：append 与 lastSeq 的组合使用需在调用方同步（见 IngestService），
 * 以保证位点分配与落盘的原子性。
 */
@Component
public class FileEventJournal implements EventJournal {

    private final Path journalFile;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<JournalEntry> entries = new ArrayList<>();
    private BufferedWriter writer;

    public FileEventJournal(RouterProperties properties) {
        this.journalFile = Path.of(properties.getDataDir(), "journal.log");
    }

    @PostConstruct
    public void init() throws IOException {
        Files.createDirectories(journalFile.getParent());
        if (Files.isRegularFile(journalFile)) {
            try (var lines = Files.lines(journalFile)) {
                lines.filter(l -> !l.isBlank()).forEach(line -> {
                    try {
                        entries.add(mapper.readValue(line, JournalEntry.class));
                    } catch (IOException e) {
                        throw new UncheckedIOException("corrupt journal line", e);
                    }
                });
            }
        }
        writer = Files.newBufferedWriter(journalFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public synchronized long append(JournalEntry entry) {
        long expected = lastSeq() + 1;
        if (entry.seq() != expected) {
            throw new IllegalArgumentException("journal seq gap: expected " + expected + " got " + entry.seq());
        }
        try {
            writer.write(mapper.writeValueAsString(entry));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append journal", e);
        }
        entries.add(entry);
        return entry.seq();
    }

    @Override
    public List<JournalEntry> readFrom(long fromSeq, int limit) {
        List<JournalEntry> out = new ArrayList<>();
        synchronized (this) {
            for (JournalEntry e : entries) {
                if (e.seq() >= fromSeq) {
                    out.add(e);
                    if (out.size() >= limit) {
                        break;
                    }
                }
            }
        }
        return out;
    }

    @Override
    public synchronized long lastSeq() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).seq();
    }
}
