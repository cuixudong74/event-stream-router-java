package com.github.cuixudong74.eventstreamrouterjava.dispatch;

import com.github.cuixudong74.eventstreamrouterjava.core.Delivery;
import com.github.cuixudong74.eventstreamrouterjava.core.JournalEntry;
import com.github.cuixudong74.eventstreamrouterjava.journal.EventJournal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单个下游的投递循环：按 seq 扫描日志、过滤命中本下游的记录、保序投递、
 * 失败按固定间隔重试、成功后推进检查点。每个下游独立线程，
 * 慢下游只影响自身滞后，不阻塞其他下游与接入端（接入端由滞后上界保护）。
 */
public class DownstreamDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DownstreamDispatcher.class);
    private static final int BATCH = 100;

    private final String downstreamId;
    private final DownstreamSink sink;
    private final EventJournal journal;
    private final CheckpointStore checkpoints;
    private final long retryBackoffMillis;
    private final AtomicLong nextSeq;
    private volatile boolean running;
    private Thread thread;

    public DownstreamDispatcher(String downstreamId, DownstreamSink sink, EventJournal journal,
                                CheckpointStore checkpoints, long startSeq, long retryBackoffMillis) {
        this.downstreamId = downstreamId;
        this.sink = sink;
        this.journal = journal;
        this.checkpoints = checkpoints;
        this.retryBackoffMillis = retryBackoffMillis;
        long cp = checkpoints.checkpoint(downstreamId).orElse(startSeq - 1);
        this.nextSeq = new AtomicLong(cp + 1);
    }

    public void start() {
        running = true;
        thread = new Thread(this::loop, "dispatcher-" + downstreamId);
        thread.setDaemon(true);
        thread.start();
    }

    public void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** 当前滞后量 = 日志最新位点 - 已确认位点。 */
    public long lag() {
        return Math.max(0, journal.lastSeq() - (nextSeq.get() - 1));
    }

    public long nextSeq() {
        return nextSeq.get();
    }

    /** 回退到指定位点重放：日志不可变，重放结果与首次投递一致。 */
    public void replay(long fromSeq) {
        checkpoints.reset(downstreamId, fromSeq - 1);
        nextSeq.set(fromSeq);
    }

    private void loop() {
        while (running) {
            try {
                List<JournalEntry> batch = journal.readFrom(nextSeq.get(), BATCH);
                if (batch.isEmpty()) {
                    sleepQuietly(20);
                    continue;
                }
                for (JournalEntry entry : batch) {
                    if (!running) {
                        return;
                    }
                    if (entry.matchedDownstreams().contains(downstreamId)) {
                        if (!deliverWithRetry(entry)) {
                            // 停止时投递未成功：不推进检查点，重启后从该位点重新投递
                            return;
                        }
                    }
                    checkpoints.save(downstreamId, entry.seq());
                    nextSeq.set(entry.seq() + 1);
                }
            } catch (Exception e) {
                log.error("dispatcher {} loop error", downstreamId, e);
                sleepQuietly(retryBackoffMillis);
            }
        }
    }

    /** 投递直至成功；返回 false 表示在成功前被停止（调用方不得推进检查点）。 */
    private boolean deliverWithRetry(JournalEntry entry) {
        long attempt = 0;
        while (running) {
            attempt++;
            try {
                sink.deliver(new Delivery(downstreamId, entry.seq(), entry.eventId(), entry.partitionKey(),
                        entry.eventType(), entry.targetSchemaVersion(), entry.convertedPayload(), attempt));
                return true;
            } catch (Exception e) {
                log.warn("deliver to {} failed at seq {} attempt {}: {}", downstreamId, entry.seq(), attempt, e.toString());
                sleepQuietly(retryBackoffMillis);
            }
        }
        return false;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
