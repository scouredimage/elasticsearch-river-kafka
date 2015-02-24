package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;
import org.apache.avro.generic.IndexedRecord;

public interface Filter {
    boolean filtered(MessageAndMetadata<String, IndexedRecord> messageAndMetadata);
}
