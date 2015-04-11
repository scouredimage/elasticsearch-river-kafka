package org.elasticsearch.river.kafka;

import com.google.protobuf.GeneratedMessage;
import kafka.message.MessageAndMetadata;

public interface Filter {
    boolean filtered(MessageAndMetadata<String, GeneratedMessage> messageAndMetadata);
}
