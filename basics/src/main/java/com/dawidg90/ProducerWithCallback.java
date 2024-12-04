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

public class ProducerWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Simple Kafka Producer with callback");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400"); //only for demo, do not use on prod

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 50; j++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("javadev", "Hi there with callback - called from iteration " + i + " [message] " + j);
                producer.send(producerRecord, (metadata, e) -> {
                    if (e == null) {
                        String timeStamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(metadata.timestamp()), ZoneId.systemDefault()).format(DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM));
                        String message = """
                        Received new metadata
                        topic: %s,
                        partition: %d,
                        offset: %d,
                        timestamp: %s
                        """
                                .formatted(metadata.topic(), metadata.partition(), metadata.offset(), timeStamp);
                        log.info(message);
                    } else {
                        log.error("Error during send a message to Kafka", e);
                    }
                });
            }
            Thread.sleep(300);
        }


        producer.flush();
        producer.close();
    }
}
