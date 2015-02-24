package org.elasticsearch.river.kafka;

import kafka.message.MessageAndMetadata;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.util.List;

public class SamplingFilter implements Filter {
    private final int percent;
    private final List<String> fields;

    public SamplingFilter(int percent, List<String> fields) {
        this.percent = percent;
        this.fields = fields;
    }

    @Override
    public boolean filtered(MessageAndMetadata<String, IndexedRecord> messageAndMetadata) {
        final IndexedRecord record = messageAndMetadata.message();
        final Schema schema = messageAndMetadata.message().getSchema();
        int hashcode = 0;
        for (String field : fields) {
            Object value = record.get(schema.getField(field).pos());
            hashcode = hashcode * 31 + ((Number) value).intValue();
        }
        return hashcode % (100 / percent) == 0;
    }

}
