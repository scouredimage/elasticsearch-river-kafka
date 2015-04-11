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
import com.google.protobuf.Parser;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer, written using Kafka Consumer Group (High Level) API, opens kafka streams to be able to consume messages.
 *
 * @author Mariam Hakobyan
 */
public class KafkaConsumer {

    private List<KafkaStream<String, GeneratedMessage>> streams;
    private ConsumerConnector consumerConnector;

    private final Parser<? extends GeneratedMessage> parser;

    private static final ESLogger logger = ESLoggerFactory.getLogger(KafkaConsumer.class.getName());

    public KafkaConsumer(final RiverConfig riverConfig) {
        try {
            Class<? extends GeneratedMessage> klass = (Class<? extends GeneratedMessage>) Class.forName((String) riverConfig.getProtoSchema());
            Field field = klass.getDeclaredField("PARSER");
            parser = (Parser) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(riverConfig));

        final Map<String, List<KafkaStream<String, GeneratedMessage>>> consumerStreams = consumerConnector.createMessageStreams(
                ImmutableMap.of(riverConfig.getTopic(), riverConfig.getConsumerThreadsPerTopic()),
                new StringDecoder(null),
                new Decoder<GeneratedMessage>() {
                    @Override
                    public GeneratedMessage fromBytes(byte[] bytes) {
                        try {
                            logger.trace("Index: {}, topic: {}: Incoming kafka message", riverConfig.getIndexName(), riverConfig.getTopic());
                            GeneratedMessage record = parser.parseFrom(bytes);
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
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "10000");
        props.put("queued.max.message.chunks", "100");
        props.put("fetch.min.bytes", "524288");
        props.put("fetch.message.max.bytes", "1048576");
        props.put("rebalance.max.retries", "10");
        props.put("rebalance.backoff.ms", "10000");
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "largest");
        props.put("num.consumer.fetchers", "1");

        return new ConsumerConfig(props);
    }

    public void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }

    List<KafkaStream<String, GeneratedMessage>> getStreams() {
        return streams;
    }

}
