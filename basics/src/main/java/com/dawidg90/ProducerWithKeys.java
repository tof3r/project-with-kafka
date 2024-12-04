package com.dawidg90;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Properties;

public class ProducerWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Simple Kafka Producer with callback");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 10; j++) {
                String topic = "javadev";
                String key = "id_" + j;
                String value = "Hi there with key - called from iteration " + j;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                producer.send(producerRecord, (metadata, e) -> {
                    if (e == null) {
                        String timeStamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(metadata.timestamp()), ZoneId.systemDefault()).format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));
                        String message = """
                            key: %s,
                            partition: %d,
                            offset: %d,
                            timestamp: %s
                            """
                                .formatted(key, metadata.partition(), metadata.offset(), timeStamp);
                        log.info(message);
                    } else {
                        log.error("Error during send a message to Kafka", e);
                    }
                });
            }
        }

        producer.flush();
        producer.close();
    }
}
