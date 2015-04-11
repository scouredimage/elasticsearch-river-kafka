package org.elasticsearch.river.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import kafka.message.MessageAndMetadata;

import java.util.List;
import java.util.Map;

public class SamplingFilter implements Filter {
    private final int percent;
    private final List<String> fields;

    public SamplingFilter(int percent, List<String> fields) {
        this.percent = percent;
        this.fields = fields;
    }

    @Override
    public boolean filtered(MessageAndMetadata<String, GeneratedMessage> messageAndMetadata) {
        final GeneratedMessage message = messageAndMetadata.message();
        int hashcode = 0;
        for (String field : fields) {
            for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
                if (field.equals(entry.getKey().getName())) {
                    hashcode = hashcode * 31 + ((Number) entry.getValue()).intValue();
                }
            }
        }
        return hashcode % (100 / percent) != 0;
    }

}
