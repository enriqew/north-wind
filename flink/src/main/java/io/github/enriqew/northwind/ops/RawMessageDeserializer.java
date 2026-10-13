package io.github.enriqew.northwind.ops;

import io.github.enriqew.northwind.model.RawMessage;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Keeps the Kafka offset and timestamp beside the bytes: they are the arrival order and arrival time. */
public class RawMessageDeserializer implements KafkaRecordDeserializationSchema<RawMessage> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawMessage> out) {
        String raw = record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);
        out.collect(new RawMessage(record.offset(), record.timestamp(), raw));
    }

    @Override
    public TypeInformation<RawMessage> getProducedType() {
        return TypeInformation.of(RawMessage.class);
    }
}
