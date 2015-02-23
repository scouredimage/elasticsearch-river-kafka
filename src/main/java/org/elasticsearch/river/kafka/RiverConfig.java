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

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.Map;

/**
 * The configuration properties that the client will provide while creating a river in elastic search.
 *
 * @author Mariam Hakobyan
 */
public class RiverConfig {

    /* Kakfa config */
    private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT = "zookeeper.connection.timeout.ms";
    private static final String TOPIC = "topic";
    private static final String CONSUMER_THREADS_PER_TOPIC = "topic.threads";

    /* Elasticsearch config */
    private static final String INDEX_NAME = "index";
    private static final String MAPPING_TYPE = "type";
    private static final String BULK_SIZE = "bulk.size";
    private static final String CONCURRENT_REQUESTS = "concurrent.requests";
    private static final String INDEXING_THREADS = "indexing.threads";
    private static final String INDEX_QUEUE_SIZE = "index.queue.size";

    /* Avro config */
    private static final String AVRO_SCHEMA = "avro.schema";
    private static final String TIMESTAMP_FIELD = "timestamp.field";

    private String zookeeperConnect;
    private int zookeeperConnectionTimeout;
    private String topic;
    private int consumerThreadsPerTopic;
    private String indexName;
    private String typeName;
    private int bulkSize;
    private int concurrentRequests;
    private int indexingThreads;
    private int indexQueueSize;
    private String avroSchema;
    private String timestampField;


    public RiverConfig(RiverName riverName, RiverSettings riverSettings) {

        // Extract kafka related configuration
        if (riverSettings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) riverSettings.settings().get("kafka");

            topic = (String) kafkaSettings.get(TOPIC);
            zookeeperConnect = XContentMapValues.nodeStringValue(kafkaSettings.get(ZOOKEEPER_CONNECT), "localhost");
            zookeeperConnectionTimeout = XContentMapValues.nodeIntegerValue(kafkaSettings.get(ZOOKEEPER_CONNECTION_TIMEOUT), 10000);
            consumerThreadsPerTopic = XContentMapValues.nodeIntegerValue(kafkaSettings.get(CONSUMER_THREADS_PER_TOPIC), 1);
        } else {
            zookeeperConnect = "localhost";
            zookeeperConnectionTimeout = 10000;
            topic = "elasticsearch-river-kafka";
        }

        // Extract ElasticSearch related configuration
        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get(INDEX_NAME), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get(MAPPING_TYPE), "status");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE), 100);
            concurrentRequests = XContentMapValues.nodeIntegerValue(indexSettings.get(CONCURRENT_REQUESTS), 1);
            indexingThreads = XContentMapValues.nodeIntegerValue(indexSettings.get(INDEXING_THREADS), 1);
            indexQueueSize = XContentMapValues.nodeIntegerValue(indexSettings.get(INDEX_QUEUE_SIZE), 10000);
            avroSchema = XContentMapValues.nodeStringValue(indexSettings.get(AVRO_SCHEMA), null);
            timestampField = XContentMapValues.nodeStringValue(indexSettings.get(TIMESTAMP_FIELD), "timestamp");
        } else {
            indexName = String.format("'%s-'yyyy-MM-dd", riverName.name());
            typeName = "status";
            bulkSize = 100;
            concurrentRequests = 1;
        }
    }

    String getTopic() {
        return topic;
    }

    String getZookeeperConnect() {
        return zookeeperConnect;
    }

    int getZookeeperConnectionTimeout() {
        return zookeeperConnectionTimeout;
    }

    public int getConsumerThreadsPerTopic() {
        return consumerThreadsPerTopic;
    }

    String getIndexName() {
        return indexName;
    }

    String getTypeName() {
        return typeName;
    }

    int getBulkSize() {
        return bulkSize;
    }

    int getConcurrentRequests() {
        return concurrentRequests;
    }

    public int getIndexingThreads() {
        return indexingThreads;
    }

    public int getIndexQueueSize() {
        return indexQueueSize;
    }

    public String getAvroSchema() {
        return avroSchema;
    }

    public String getTimestampField() {
        return timestampField;
    }
}
