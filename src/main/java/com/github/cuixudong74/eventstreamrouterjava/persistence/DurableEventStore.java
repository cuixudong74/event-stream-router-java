package com.github.cuixudong74.eventstreamrouterjava.persistence;

import com.github.cuixudong74.eventstreamrouterjava.model.DeadLetterRecord;

import java.util.List;
import java.util.Optional;

public interface DurableEventStore {
    StoredEvent append(StoredEvent event);
    Optional<StoredEvent> read(long sequence);
    List<StoredEvent> range(long fromSequenceInclusive, long toSequenceExclusive);
    long nextSequence();
    List<StoredEvent> all();
    void commit(String downstreamId, long sequence, boolean delivered);
    DownstreamProgress progress(String downstreamId);
    List<DownstreamProgress> progressList();
    void addDownstream(DownstreamProgress progress);
    void updateProgress(DownstreamProgress progress);
    void addDeadLetter(DeadLetterRecord record);
    List<DeadLetterRecord> deadLetters();
}
