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

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer, written using Kafka Consumer Group (High Level) API, opens kafka streams to be able to consume messages.
 *
 * @author Mariam Hakobyan
 */
public class KafkaConsumer {

    private List<KafkaStream<String, IndexedRecord>> streams;
    private ConsumerConnector consumerConnector;

    private final Schema schema;
    private final GenericDatumReader<IndexedRecord> datumReader;
    private final DecoderFactory decoderFactory;

    private static final ESLogger logger = ESLoggerFactory.getLogger(KafkaConsumer.class.getName());


    public KafkaConsumer(final RiverConfig riverConfig) {
        try {
            schema = ReflectData.get().getSchema(Class.forName((String) riverConfig.getAvroSchema()));
            datumReader = new GenericDatumReader<IndexedRecord>(schema);
            decoderFactory = DecoderFactory.get();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(riverConfig));

        final Map<String, List<KafkaStream<String, IndexedRecord>>> consumerStreams = consumerConnector.createMessageStreams(
                ImmutableMap.of(riverConfig.getTopic(), riverConfig.getConsumerThreadsPerTopic()),
                new StringDecoder(null),
                new Decoder<IndexedRecord>() {
                    @Override
                    public IndexedRecord fromBytes(byte[] bytes) {
                        try {
                            logger.trace("Index: {}, topic: {}: Incoming kafka message", riverConfig.getIndexName(), riverConfig.getTopic());

                            BinaryDecoder decoder = decoderFactory.binaryDecoder(bytes, null);
                            final IndexedRecord record = datumReader.read(null, decoder);
                            logger.trace("Index: {}, topic: {}: Decoded kafka message: {}", riverConfig.getIndexName(), riverConfig.getTopic(), record);

                            return record;
                        } catch (IOException e) {
                            logger.warn("Error decoding kafka message", e);
                            return null;
                        }
                    }
                });

        streams = consumerStreams.get(riverConfig.getTopic());

        logger.debug("Index: {}: Started kafka consumer for topic: {} with {} partitions in it.",
                riverConfig.getIndexName(), riverConfig.getTopic(), streams.size());
    }

    private ConsumerConfig createConsumerConfig(final RiverConfig riverConfig) {
        final Properties props = new Properties();
        props.put("group.id", "river-" + riverConfig.getTopic());
        props.put("zookeeper.connect", riverConfig.getZookeeperConnect());
        props.put("zookeeper.connection.timeout.ms", String.valueOf(riverConfig.getZookeeperConnectionTimeout()));
        props.put("zookeeper.session.timeout.ms", "60000");
        props.put("zookeeper.sync.time.ms", "10000");
        props.put("queued.max.message.chunks", "500");
        props.put("fetch.message.max.bytes", "1048576");
        props.put("rebalance.max.retries", "10");
        props.put("rebalance.backoff.ms", "5000");
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "largest");
        props.put("num.consumer.fetchers", "5");

        return new ConsumerConfig(props);
    }

    public void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }

    List<KafkaStream<String, IndexedRecord>> getStreams() {
        return streams;
    }

    public Schema getSchema() {
        return schema;
    }
}
