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

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.ThreadFactoryBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class KafkaWorkerPool {

    private KafkaConsumer kafkaConsumer;
    private RiverConfig riverConfig;
    final Queue<byte[]> queue;

    private volatile boolean consume = false;
    private ExecutorService executor;

    private final EncoderFactory encoderFactory;
    private final GenericDatumWriter<Object> datumWriter;

    private static final ESLogger logger = ESLoggerFactory.getLogger(KafkaWorkerPool.class.getName());


    public KafkaWorkerPool(final KafkaConsumer kafkaConsumer,
                           final RiverConfig riverConfig,
                           final Queue<byte[]> queue) {
        this.kafkaConsumer = kafkaConsumer;
        this.riverConfig = riverConfig;
        this.queue = queue;

        this.encoderFactory = EncoderFactory.get();
        datumWriter = new GenericDatumWriter<Object>(this.kafkaConsumer.getSchema());
    }

    public void start() {

        logger.debug("Index: {}: Kafka worker started...", riverConfig.getIndexName());

        if (consume) {
            logger.debug("Index: {}: Consumer is already running, new one will not be started...", riverConfig.getIndexName());
            return;
        }

        consume = true;
        logger.debug("Index: {}: Kafka consumer started...", riverConfig.getIndexName());

        List<KafkaStream<String, IndexedRecord>> streams = kafkaConsumer.getStreams();
        executor = Executors.newFixedThreadPool(
                riverConfig.getConsumerThreadsPerTopic(),
                new ThreadFactoryBuilder().setNameFormat("kafka-river-consumer-%d").build());
        for (final KafkaStream<String, IndexedRecord> stream : streams) {
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
    private void consumeMessagesAndAddToQueue(final KafkaStream<String, IndexedRecord> stream) {
        logger.debug("Index: {}: Consuming from topic: {}", riverConfig.getIndexName(), riverConfig.getTopic());
        final ConsumerIterator<String, IndexedRecord> consumerIterator = stream.iterator();

        // Consume all the messages of the stream (partition)
        while (consumerIterator.hasNext()) {
            if (!consume) {
                break;
            }
            final MessageAndMetadata<String, IndexedRecord> messageAndMetadata = consumerIterator.next();
            if (logger.isTraceEnabled()) {
                logMessage(messageAndMetadata);
            }

            byte[] json;
            try {
                ByteArrayOutputStream os = new ByteArrayOutputStream(64);
                JsonEncoder encoder = encoderFactory.jsonEncoder(kafkaConsumer.getSchema(), os);

                datumWriter.write(messageAndMetadata.message(), encoder);
                encoder.flush();

                json = os.toByteArray();
            } catch (IOException e) {
                logger.warn("Index: {}, topic: {}: Error encoding to JSON", e, riverConfig.getIndexName(), riverConfig.getTopic());
                continue;
            }

            while (!queue.offer(json)) {
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
        logger.info("Index: {}, consume={}: Kafka consumer done!", riverConfig.getIndexName(), consume);
    }

    /**
     * Logs consumed kafka messages to the log.
     */
    private void logMessage(final MessageAndMetadata messageAndMetadata) {
        final byte[] messageBytes = (byte[]) messageAndMetadata.message();
        try {
            logger.trace("Index: {}: Message received: {}", riverConfig.getIndexName(), new String(messageBytes, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            logger.trace("The UTF-8 charset is not supported for the kafka message.");
        }
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
