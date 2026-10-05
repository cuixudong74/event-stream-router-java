package com.github.cuixudong74.eventstreamrouterjava.journal;

import com.github.cuixudong74.eventstreamrouterjava.core.JournalEntry;

import java.util.List;

/**
 * 已提交事件的只追加日志，是恢复与重放的唯一事实来源。
 */
public interface EventJournal {

    /** 追加一条已提交记录，返回其位点。 */
    long append(JournalEntry entry);

    /** 从 fromSeq（含）开始按序读取，最多 limit 条。 */
    List<JournalEntry> readFrom(long fromSeq, int limit);

    /** 当前已提交的最大位点；空日志返回 0。 */
    long lastSeq();
}
