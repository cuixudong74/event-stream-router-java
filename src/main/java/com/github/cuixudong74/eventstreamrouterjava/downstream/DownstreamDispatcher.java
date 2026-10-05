package com.github.cuixudong74.eventstreamrouterjava.downstream;

import com.github.cuixudong74.eventstreamrouterjava.service.BackpressureException;
import com.github.cuixudong74.eventstreamrouterjava.model.BackpressurePolicy;
import com.github.cuixudong74.eventstreamrouterjava.model.DeadLetterRecord;
import com.github.cuixudong74.eventstreamrouterjava.model.DownstreamMetrics;
import com.github.cuixudong74.eventstreamrouterjava.model.DownstreamState;
import com.github.cuixudong74.eventstreamrouterjava.persistence.DownstreamProgress;
import com.github.cuixudong74.eventstreamrouterjava.persistence.FileDurableEventStore;
import com.github.cuixudong74.eventstreamrouterjava.persistence.StoredEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class DownstreamDispatcher {
    private final FileDurableEventStore store;
    private final DownstreamRegistry registry;
    private final ConcurrentMap<String, WorkerState> workers = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "downstream-dispatcher");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DownstreamDispatcher(FileDurableEventStore store, DownstreamRegistry registry) {
        this.store = store;
        this.registry = registry;
    }

    public synchronized void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        for (DownstreamProgress progress : store.progressList()) {
            if (progress.state() != DownstreamState.REMOVED) {
                startWorker(progress, recoverEvents(progress));
            }
        }
    }

    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                Thread.currentThread().interrupt();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            workers.clear();
        }
    }

    public synchronized boolean prepare(StoredEvent event) {
        if (!running.get()) {
            throw new IllegalStateException("dispatcher is not running");
        }
        String destination = event.decision().destinationId();
        DownstreamProgress progress = store.progress(destination);
        if (progress.state() != DownstreamState.ACTIVE) {
            throw new IllegalStateException("downstream " + destination + " is not active");
        }
        WorkerState state = workers.get(destination);
        if (state == null) {
            throw new IllegalStateException("worker is not registered for downstream " + destination);
        }

        try {
            if (progress.backpressurePolicy() == BackpressurePolicy.BLOCK_PRODUCER) {
                state.capacity.acquire();
                return true;
            }
            boolean accepted = state.capacity.tryAcquire(100, TimeUnit.MILLISECONDS);
            if (!accepted && progress.backpressurePolicy() == BackpressurePolicy.REJECT_PRODUCER) {
                throw new BackpressureException("downstream " + destination + " is at capacity "
                        + progress.queueCapacity());
            }
            return accepted;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while enqueuing event", e);
        }
    }

    public synchronized void publish(StoredEvent event) {
        WorkerState state = workers.get(event.decision().destinationId());
        if (state == null) {
            throw new IllegalStateException("worker is not registered for downstream "
                    + event.decision().destinationId());
        }
        try {
            state.queue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing durable event", e);
        }
    }

    public synchronized void rollback(StoredEvent event) {
        WorkerState state = workers.get(event.decision().destinationId());
        if (state != null) {
            state.capacity.release();
        }
    }

    public synchronized void deadLetter(StoredEvent event, String reason) {
        store.addDeadLetter(new DeadLetterRecord(event.sequence(), event.targetEvent().eventId(),
                event.decision().destinationId(), reason, Instant.now()));
    }

    public synchronized void addDownstream(DownstreamProgress progress) {
        store.addDownstream(progress);
        if (running.get()) {
            startWorker(progress, recoverEvents(progress));
        }
    }

    public synchronized void replay(String downstreamId, long fromSequence) {
        DownstreamProgress current = store.progress(downstreamId);
        if (current.state() != DownstreamState.ACTIVE) {
            throw new IllegalStateException("only active downstreams can be replayed");
        }
        stopWorker(downstreamId);
        long committed = Math.max(current.startSequence() - 1, fromSequence - 1);
        DownstreamProgress reset = new DownstreamProgress(current.downstreamId(), current.startSequence(),
                committed, current.queueCapacity(), current.backpressurePolicy(), current.state(),
                current.deliveredCount(), current.deadLetterCount());
        store.updateProgress(reset);
        if (running.get()) {
            startWorker(reset, recoverEvents(reset));
        }
    }

    public synchronized void remove(String downstreamId) throws InterruptedException {
        DownstreamProgress current = store.progress(downstreamId);
        DownstreamProgress draining = new DownstreamProgress(current.downstreamId(), current.startSequence(),
                current.committedSequence(), current.queueCapacity(), current.backpressurePolicy(),
                DownstreamState.DRAINING, current.deliveredCount(), current.deadLetterCount());
        store.updateProgress(draining);

        while (true) {
            WorkerState state = workers.get(downstreamId);
            if (state == null || (state.queue.isEmpty() && state.inFlightSequence.get() == null)) {
                break;
            }
            wait(100);
        }

        stopWorker(downstreamId);
        DownstreamProgress removed = new DownstreamProgress(draining.downstreamId(), draining.startSequence(),
                draining.committedSequence(), draining.queueCapacity(), draining.backpressurePolicy(),
                DownstreamState.REMOVED, draining.deliveredCount(), draining.deadLetterCount());
        store.updateProgress(removed);
    }

    public List<DownstreamMetrics> metrics() {
        long latest = store.nextSequence() - 1;
        List<DownstreamMetrics> result = new ArrayList<>();
        for (DownstreamProgress progress : store.progressList()) {
            WorkerState state = workers.get(progress.downstreamId());
            int queueSize = state == null ? 0 : state.queue.size();
            Long inFlight = state == null ? null : state.inFlightSequence.get();
            result.add(new DownstreamMetrics(
                    progress.downstreamId(),
                    progress.state().name(),
                    progress.startSequence(),
                    progress.committedSequence(),
                    latest,
                    queueSize,
                    inFlight != null,
                    inFlight,
                    lag(progress),
                    progress.deliveredCount(),
                    progress.deadLetterCount()));
        }
        return result;
    }

    private long lag(DownstreamProgress progress) {
        return store.all().stream()
                .filter(event -> event.decision().destinationId().equals(progress.downstreamId()))
                .filter(event -> event.sequence() >= progress.startSequence())
                .filter(event -> event.sequence() > progress.committedSequence())
                .filter(event -> store.deadLetters().stream().noneMatch(dl ->
                        dl.sequence() == event.sequence() && dl.downstreamId().equals(progress.downstreamId())))
                .count();
    }

    private List<StoredEvent> recoverEvents(DownstreamProgress progress) {
        return store.all().stream()
                .filter(event -> event.decision().destinationId().equals(progress.downstreamId()))
                .filter(event -> event.sequence() >= progress.startSequence())
                .filter(event -> event.sequence() > progress.committedSequence())
                .filter(event -> store.deadLetters().stream().noneMatch(dl ->
                        dl.sequence() == event.sequence() && dl.downstreamId().equals(progress.downstreamId())))
                .toList();
    }

    private void startWorker(DownstreamProgress progress, List<StoredEvent> initialEvents) {
        WorkerState state = new WorkerState(progress.queueCapacity(), initialEvents.size());
        state.queue.addAll(initialEvents);
        workers.put(progress.downstreamId(), state);
        Future<?> future = executor.submit(() -> runWorker(progress.downstreamId(), state));
        state.future = future;
    }

    private void stopWorker(String downstreamId) {
        WorkerState state = workers.remove(downstreamId);
        if (state != null && state.future != null) {
            state.future.cancel(true);
        }
    }

    private void runWorker(String downstreamId, WorkerState state) {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                StoredEvent event = state.queue.poll(100, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                state.inFlightSequence.set(event.sequence());
                deliverWithRetry(downstreamId, event);
                store.commit(downstreamId, event.sequence(), true);
                state.capacity.release();
                state.inFlightSequence.set(null);
                signalDrainIfEmpty(downstreamId, state);
                DownstreamProgress progress = store.progress(downstreamId);
                if (progress.state() == DownstreamState.DRAINING && state.queue.isEmpty()) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                state.inFlightSequence.set(null);
            }
        }
    }

    private void deliverWithRetry(String downstreamId, StoredEvent event) throws InterruptedException {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                DownstreamReceiver receiver = registry.find(downstreamId)
                        .orElseThrow(() -> new IllegalStateException("receiver is not registered"));
                receiver.receive(event.targetEvent(), event.decision());
                return;
            } catch (RuntimeException retryable) {
                Thread.sleep(50);
            }
        }
        throw new InterruptedException();
    }

    private synchronized void signalDrainIfEmpty(String downstreamId, WorkerState state) {
        if (state.queue.isEmpty() && state.inFlightSequence.get() == null) {
            DownstreamProgress progress = store.progress(downstreamId);
            if (progress.state() == DownstreamState.DRAINING) {
                notifyAll();
            }
        }
    }

    private static final class WorkerState {
        private final BlockingQueue<StoredEvent> queue;
        private final Semaphore capacity;
        private final AtomicReference<Long> inFlightSequence = new AtomicReference<>();
        private volatile Future<?> future;

        private WorkerState(int capacity, int initialSize) {
            this.queue = new LinkedBlockingQueue<>(Math.max(capacity, initialSize));
            int outstandingPermits = Math.max(0, capacity - initialSize);
            this.capacity = new Semaphore(outstandingPermits);
        }
    }
}
