package org.elasticsearch.river.simple;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleRiver extends AbstractRiverComponent implements River {

    private final Client client;
    private final String indexName;
    private final String typeName;
    private final int bulkSize;
    private int simpleNumber;
    private String fieldName;
    private volatile BulkRequestBuilder currentRequest;
    private volatile boolean closed = false;
    private TimeValue poll;
    private Thread thread;
    private AtomicInteger onGoingBulks = new AtomicInteger();
    private int bulkThreshold;

    @SuppressWarnings({"unchecked"})
    @Inject
    public SimpleRiver(RiverName riverName, RiverSettings settings, Client client, ThreadPool threadPool) {
        super(riverName, settings);
        this.client = client;

        if (settings.settings().containsKey("simple")) {
            Map<String, Object> simpleSettings = (Map<String, Object>) settings.settings().get("simple");
            simpleNumber = XContentMapValues.nodeIntegerValue(simpleSettings.get("number"), 100);
            fieldName = XContentMapValues.nodeStringValue(simpleSettings.get("field"), "test");
            poll = XContentMapValues.nodeTimeValue(simpleSettings.get("poll"), TimeValue.timeValueMinutes(60));
       }

        logger.info("creating simple stream river for [{} numbers] with field [{}]", simpleNumber, fieldName);

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "simple_type");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            bulkThreshold = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_threshold"), 10);
        } else {
            indexName = riverName.name();
            typeName = "simple_type";
            bulkSize = 100;
            bulkThreshold = 10;
        }

    }

    @Override
    public void start() {
        logger.info("starting simple stream");
        currentRequest = client.prepareBulk();
        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "Simple processor").newThread(new SimpleConnector());
        thread.start();
    }

    @Override
    public void close() {
        logger.info("closing simple stream river");
        this.closed = true;
        thread.interrupt();
    }

    private void delay() {
        if (poll.millis() > 0L) {
            logger.info("next run waiting for {}", poll);
            try {
                Thread.sleep(poll.millis());
            } catch (InterruptedException e) {
                logger.error("Error during waiting.", e, (Object) null);
            }
        }
    }


    private void processBulkIfNeeded() {
        if (currentRequest.numberOfActions() >= bulkSize) {
            // execute the bulk operation
            int currentOnGoingBulks = onGoingBulks.incrementAndGet();
            if (currentOnGoingBulks > bulkThreshold) {
                onGoingBulks.decrementAndGet();
                logger.warn("ongoing bulk, [{}] crossed threshold [{}], waiting", onGoingBulks, bulkThreshold);
                try {
                    synchronized (this) {
                        wait();
                    }
                } catch (InterruptedException e) {
                    logger.error("Error during wait", e);
                }
            }
            {
                try {
                    currentRequest.execute(new ActionListener<BulkResponse>() {
                        @Override
                        public void onResponse(BulkResponse bulkResponse) {
                            onGoingBulks.decrementAndGet();
                            notifySimpleRiver();
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            onGoingBulks.decrementAndGet();
                            notifySimpleRiver();
                            logger.warn("failed to execute bulk");
                        }
                    });
                } catch (Exception e) {
                    onGoingBulks.decrementAndGet();
                    notifySimpleRiver();
                    logger.warn("failed to process bulk", e);
                }
            }
            currentRequest = client.prepareBulk();
        }
    }

    private void notifySimpleRiver() {
        synchronized (SimpleRiver.this) {
            SimpleRiver.this.notify();
        }
    }

    private class SimpleConnector implements Runnable {

        @Override
        public void run() {
            while (!closed) {
                try {
                    for(int i=0; i<simpleNumber; i++){
                        XContentBuilder builder = XContentFactory.jsonBuilder();
                        builder.startObject();

                        builder.field(fieldName, i);
                        builder.endObject();
                        currentRequest.add(Requests.indexRequest(indexName).type(typeName).id(UUID.randomUUID().toString()).create(true).source(builder));
                        processBulkIfNeeded();
                    }
                    if(currentRequest.numberOfActions()>0){
                        currentRequest.execute().get();
                        currentRequest = client.prepareBulk();
                    }
                    delay();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e, (Object) null);
                    closed = true;
                }
                if (closed) {
                    return;
                }
            }
        }
    }
}
