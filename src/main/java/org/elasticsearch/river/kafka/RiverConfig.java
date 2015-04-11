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

import java.util.ArrayList;
import java.util.List;
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
    private static final String PROTO_SCHEMA = "proto.schema";
    private static final String TIMESTAMP_FIELD = "timestamp.field";

    /* Sampling */
    private static final String SAMPLE_PERCENT = "percent";
    private static final String SAMPLE_FIELDS = "fields";

    /* Filtering */
    private static final String FILTER_FIELD = "field";
    private static final String FILTER_VALUES = "values";

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
    private String protoSchema;
    private String timestampField;

    private boolean sampled = false;
    private int samplePercent;
    private List<String> sampleFields;

    private boolean filtered = false;
    private String filterField;
    private List<Object> filterValues;

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
            protoSchema = XContentMapValues.nodeStringValue(indexSettings.get(PROTO_SCHEMA), null);
            timestampField = XContentMapValues.nodeStringValue(indexSettings.get(TIMESTAMP_FIELD), "timestamp");
        } else {
            indexName = String.format("'%s-'yyyy-MM-dd", riverName.name());
            typeName = "status";
            bulkSize = 100;
            concurrentRequests = 1;
        }

        if (riverSettings.settings().containsKey("sampled")) {
            Map<String, Object> sampleSettings = (Map<String, Object>) riverSettings.settings().get("sampled");
            samplePercent = XContentMapValues.nodeIntegerValue(sampleSettings.get(SAMPLE_PERCENT), 10);
            List<Object> fields = (List) sampleSettings.get(SAMPLE_FIELDS);
            sampleFields = new ArrayList<String>(fields.size());
            for (Object field : fields) {
                sampleFields.add(field.toString());
            }
            if (sampleFields.size() < 1) {
                throw new RuntimeException("Configuration error! Must specify fields to use when sampled.");
            }
            sampled = true;
        } else if (riverSettings.settings().containsKey("filter")) {
            Map<String, Object> filterSettings = (Map<String, Object>) riverSettings.settings().get("filter");
            filterField = XContentMapValues.nodeStringValue(filterSettings.get(FILTER_FIELD), null);
            filterValues = (List) filterSettings.get(FILTER_VALUES);
            if (filterField == null || (filterValues == null || filterValues.isEmpty())) {
                throw new RuntimeException("Configuration error! Must specify filter field and match values.");
            }
            filtered = true;
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

    public String getProtoSchema() {
        return protoSchema;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public boolean isSampled() {
        return sampled;
    }

    public int getSamplePercent() {
        return samplePercent;
    }

    public List<String> getSampleFields() {
        return sampleFields;
    }

    public boolean isFiltered() {
        return filtered;
    }

    public String getFilterField() {
        return filterField;
    }

    public List<Object> getFilterValues() {
        return filterValues;
    }
}
