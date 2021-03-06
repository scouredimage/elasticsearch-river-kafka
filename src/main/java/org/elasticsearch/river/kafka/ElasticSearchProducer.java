/*
 * Copyright 2014 Mariam Hakobyan
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

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Queue;

/**
 * An ElasticSearch base producer, which creates an index, mapping in the EL.
 * Also, creates index/delete document requests against ElasticSearch, and executes them with Bulk API.
 *
 * @author Mariam Hakobyan
 */
public class ElasticSearchProducer implements Runnable {

    private static final ESLogger logger = ESLoggerFactory.getLogger(ElasticSearchProducer.class.getName());

    private final RiverConfig riverConfig;

    private final BulkProcessor bulkProcessor;
    private final Queue<String> queue;
    private final ObjectReader reader;

    public static volatile boolean PROCESS = true;

    public ElasticSearchProducer(final RiverConfig riverConfig,
                                 final Queue<String> queue,
                                 final Client client) {
        this.riverConfig = riverConfig;
        this.queue = queue;
        this.bulkProcessor = createBulkProcessor(client);
        this.reader = new ObjectMapper().reader(new TypeReference<Map<String, Object>>() {});
    }

    private BulkProcessor createBulkProcessor(final Client client) {
        return BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.debug("Index: {}: Going to execute bulk request composed of {} actions.",
                                riverConfig.getIndexName(), request.numberOfActions());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.info("Index: {}: Executed bulk composed of {} actions.",
                                riverConfig.getIndexName(), request.numberOfActions());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.warn("Index: {}: Error executing bulk.", failure, riverConfig.getIndexName());
                    }
                })
                .setBulkActions(riverConfig.getBulkSize())
                .setBulkSize(new ByteSizeValue(-1L))
                .setConcurrentRequests(riverConfig.getConcurrentRequests())
                .build();
    }

    public void run() {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(riverConfig.getIndexName());
        while (PROCESS) {
            try {
                String json = queue.poll();
                if (json != null) {
                    logger.trace("Index: {}, topic: {}: Incoming index request: {}",
                            riverConfig.getIndexName(), riverConfig.getTopic(), json);

                    final Map<String, Object> messageMap = reader.readValue(json);
                    logger.trace("Index: {}, topic: {}: Decoded JSON: {} from message: {}",
                            riverConfig.getIndexName(), riverConfig.getTopic(), messageMap, json);

                    Long timestamp = (Long) messageMap.get(riverConfig.getTimestampField());

                    IndexRequest indexRequest = Requests
                            .indexRequest(dateFormat.format(timestamp == null ? new Date() : new Date(timestamp)))
                            .type(riverConfig.getTypeName())
                            .source(messageMap);
                    if (timestamp != null) {
                        indexRequest.timestamp(String.valueOf(timestamp));
                    }

                    bulkProcessor.add(indexRequest);
                    logger.trace("Index: {}, topic: {}: index request: {} submitted",
                            riverConfig.getIndexName(), riverConfig.getTopic(), messageMap);

                } else {
                    if (!PROCESS) {
                        break;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Exception ex) {
                logger.warn("Error parsing message", ex);
            }
        }
        logger.info("Index: {}, topic: {}, PROCESS: {}: Search provider done!",
                riverConfig.getIndexName(), riverConfig.getTopic(), PROCESS);
    }

}
