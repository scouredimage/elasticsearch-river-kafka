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

import com.google.protobuf.GeneratedMessage;
import com.googlecode.protobuf.format.JsonFormat;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.ThreadFactoryBuilder;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class KafkaWorkerPool {

    private KafkaConsumer kafkaConsumer;
    private RiverConfig riverConfig;
    final Queue<String> queue;

    private volatile boolean consume = false;
    private ExecutorService executor;

    private final Filter filter;

    private static final ESLogger logger = ESLoggerFactory.getLogger(KafkaWorkerPool.class.getName());


    public KafkaWorkerPool(final KafkaConsumer kafkaConsumer,
                           final RiverConfig riverConfig,
                           final Queue<String> queue,
                           final Filter filter) {
        this.kafkaConsumer = kafkaConsumer;
        this.riverConfig = riverConfig;
        this.queue = queue;
        this.filter = filter;
    }

    public void start() {

        logger.debug("Index: {}: Kafka worker started...", riverConfig.getIndexName());

        if (consume) {
            logger.debug("Index: {}: Consumer is already running, new one will not be started...", riverConfig.getIndexName());
            return;
        }

        consume = true;
        logger.debug("Index: {}: Kafka consumer started...", riverConfig.getIndexName());

        List<KafkaStream<String, GeneratedMessage>> streams = kafkaConsumer.getStreams();
        executor = Executors.newFixedThreadPool(
                riverConfig.getConsumerThreadsPerTopic(),
                new ThreadFactoryBuilder().setNameFormat("kafka-river-consumer-%d").build());
        for (final KafkaStream<String, GeneratedMessage> stream : streams) {
            executor.submit(
                    new Runnable() {
                        @Override
                            public void run() {
                                consumeMessagesAndAddToQueue(stream);
                                logger.info("Index: {}, topic: {}: Done with kafka stream: {}",
                                        riverConfig.getIndexName(), riverConfig.getTopic(), stream);
                            }
                        });
        }
    }

    /**
     * Consumes the messages from the partition via specified stream.
     */
    private void consumeMessagesAndAddToQueue(final KafkaStream<String, GeneratedMessage> stream) {
        logger.debug("Index: {}: Consuming from topic: {}", riverConfig.getIndexName(), riverConfig.getTopic());
        final ConsumerIterator<String, GeneratedMessage> consumerIterator = stream.iterator();

        // Consume all the messages of the stream (partition)
        while (consumerIterator.hasNext()) {
            if (!consume) {
                break;
            }

            final MessageAndMetadata<String, GeneratedMessage> messageAndMetadata = consumerIterator.next();
            if (messageAndMetadata == null) {
                continue;
            }
            logger.trace("Index: {}, topic: {}: Incoming message: {}",
                    riverConfig.getIndexName(), riverConfig.getTopic(), messageAndMetadata.message());
            if (filter != null && filter.filtered(messageAndMetadata)) {
                logger.debug("Index: {}, topic: {}: Message filtered: {}",
                        riverConfig.getIndexName(), riverConfig.getTopic(), messageAndMetadata.message());
            }
            else {
                while (!queue.offer(JsonFormat.printToString(messageAndMetadata.message()))) {
                    if (!consume) {
                        break;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        logger.info("Index: {}, consume={}: Kafka consumer done!", riverConfig.getIndexName(), consume);
    }

    void stop() {
        consume = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(90, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(10, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
