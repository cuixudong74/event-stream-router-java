package com.github.cuixudong74.eventstreamrouterjava.tests;

import com.github.cuixudong74.eventstreamrouterjava.config.RouterProperties;
import com.github.cuixudong74.eventstreamrouterjava.convert.ConversionRegistry;
import com.github.cuixudong74.eventstreamrouterjava.convert.EventConverter;
import com.github.cuixudong74.eventstreamrouterjava.core.Delivery;
import com.github.cuixudong74.eventstreamrouterjava.core.EventEnvelope;
import com.github.cuixudong74.eventstreamrouterjava.dispatch.CheckpointStore;
import com.github.cuixudong74.eventstreamrouterjava.dispatch.DownstreamManager;
import com.github.cuixudong74.eventstreamrouterjava.dispatch.DownstreamSink;
import com.github.cuixudong74.eventstreamrouterjava.journal.FileEventJournal;
import com.github.cuixudong74.eventstreamrouterjava.routing.RoutingTable;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaRegistry;
import com.github.cuixudong74.eventstreamrouterjava.schema.SchemaValidator;
import com.github.cuixudong74.eventstreamrouterjava.service.IngestService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试基座：手工组装各组件（绕过 Spring 容器），可模拟重启（同一目录新建实例）。
 */
class TestHarness implements AutoCloseable {

    final RouterProperties props;
    final SchemaRegistry schemas;
    final ConversionRegistry conversions;
    final EventConverter converter;
    final RoutingTable routing;
    final FileEventJournal journal;
    final CheckpointStore checkpoints;
    final DownstreamManager downstreams;
    final IngestService ingest;

    TestHarness(Path dir) {
        this(dir, 10_000);
    }

    TestHarness(Path dir, long maxLag) {
        props = new RouterProperties();
        props.setDataDir(dir.toString());
        props.setMaxLag(maxLag);
        props.setRetryBackoffMillis(20);
        schemas = new SchemaRegistry(props);
        conversions = new ConversionRegistry(props);
        converter = new EventConverter(conversions);
        routing = new RoutingTable(props);
        journal = new FileEventJournal(props);
        try {
            journal.init();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        checkpoints = new CheckpointStore(props);
        downstreams = new DownstreamManager(journal, checkpoints, props);
        ingest = new IngestService(schemas, new SchemaValidator(), converter, routing, journal, downstreams, props);
    }

    /** 模拟重启：关闭当前实例（停止投递线程），调用方随后用同一目录新建实例。 */
    @Override
    public void close() {
        downstreams.shutdown();
    }

    long submit(String eventId, int version, String partitionKey, String eventType, Map<String, Object> payload) {
        return ingest.ingest(new EventEnvelope(eventId, version, partitionKey, eventType, payload,
                System.currentTimeMillis())).seq();
    }

    /** 录制投递的 sink，可注入延迟与失败。 */
    static class RecordingSink implements DownstreamSink {
        final List<Delivery> deliveries = new CopyOnWriteArrayList<>();
        volatile long delayMillis = 0;
        final AtomicInteger failuresBeforeSuccess = new AtomicInteger(0);

        @Override
        public void deliver(Delivery delivery) throws Exception {
            if (delayMillis > 0) {
                Thread.sleep(delayMillis);
            }
            if (failuresBeforeSuccess.getAndDecrement() > 0) {
                throw new RuntimeException("injected failure");
            }
            deliveries.add(delivery);
        }

        List<String> eventIds() {
            return deliveries.stream().map(Delivery::eventId).toList();
        }
    }
}
