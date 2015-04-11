package org.elasticsearch.river.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import kafka.message.MessageAndMetadata;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MatchFilter implements Filter {
    private static final ESLogger logger = ESLoggerFactory.getLogger(MatchFilter.class.getName());

    private final String field;
    private final Set<Object> values;

    public MatchFilter(String field, Collection<Object> values) {
        this.field = field;
        this.values = new HashSet<Object>(values);
    }

    @Override
    public boolean filtered(MessageAndMetadata<String, GeneratedMessage> messageAndMetadata) {
        logger.trace("Filter? {}", messageAndMetadata);
        final GeneratedMessage message = messageAndMetadata.message();
        logger.trace("Filter {} for {}", field, values);
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            if (field.equals(entry.getKey().getName())) {
                if (values.contains(entry.getValue())) {
                    logger.debug("Filter matched! ({}, {})", entry.getKey().getName(), entry.getValue());
                    return false;
                }
                logger.debug("Field value not matched! ({}, {})", entry.getKey().getName(), entry.getValue());
                return true;
            }
        }
        logger.debug("Field {} not found!", field);
        return true;
    }
}
