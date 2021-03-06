/*
 * Copyright 2013 Mariam Hakobyan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.river.kafka;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadFactoryBuilder;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This is the actual river implementation, which starts a workerThread to read messages from kafka and put them into elasticsearch.
 */
public class KafkaRiver extends AbstractRiverComponent implements River {

    private KafkaConsumer kafkaConsumer;
    private RiverConfig riverConfig;
    final Queue<String> queue;

    private KafkaWorkerPool kafkaWorkerPool;

    private final Client client;
    private ExecutorService providerService;

    @Inject
    protected KafkaRiver(final RiverName riverName, final RiverSettings riverSettings, final Client client) {
        super(riverName, riverSettings);

        riverConfig = new RiverConfig(riverName, riverSettings);
        kafkaConsumer = new KafkaConsumer(riverConfig);
        queue = new ArrayBlockingQueue<String>(riverConfig.getIndexQueueSize());
        this.client = client;
    }

    @Override
    public void start() {
        try {
            logger.debug("Index: {}: Starting Kafka River...", riverConfig.getIndexName());

            final Filter filter = riverConfig.isSampled()
                    ? new SamplingFilter(riverConfig.getSamplePercent(), riverConfig.getSampleFields())
                    : (riverConfig.isFiltered()
                        ? new MatchFilter(riverConfig.getFilterField(), riverConfig.getFilterValues())
                        : null);
            kafkaWorkerPool = new KafkaWorkerPool(kafkaConsumer, riverConfig, queue, filter);
            kafkaWorkerPool.start();

            providerService = Executors.newFixedThreadPool(
                    riverConfig.getIndexingThreads(),
                    new ThreadFactoryBuilder().setNameFormat("kafka-river-indexer-%d").build());
            for (int i = 0; i < riverConfig.getIndexingThreads(); i++) {
                providerService.submit(new ElasticSearchProducer(riverConfig, queue, client));
            }
        } catch (Exception ex) {
            logger.error("Index: {}: Unexpected Error occurred", ex, riverConfig.getIndexName());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        logger.debug("Index: {}: Closing kafka river...", riverConfig.getIndexName());
        kafkaConsumer.shutdown();
        kafkaWorkerPool.stop();
        ElasticSearchProducer.PROCESS = false;
        stopProcessorService();
    }

    private void stopProcessorService() {
        providerService.shutdown();
        try {
            if (!providerService.awaitTermination(90, TimeUnit.MILLISECONDS)) {
                providerService.shutdownNow();
                providerService.awaitTermination(10, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            providerService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
