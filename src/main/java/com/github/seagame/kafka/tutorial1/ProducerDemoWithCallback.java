package com.github.seagame.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";
        System.out.println("Create Producer Properties");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.out.println("Create The Producer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create Producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "helo_world");

        System.out.println("Send data");
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("Received new metadata. \n"+
                            "Topic: "+ recordMetadata.topic() + "\n" +
                            "Partition: "+ recordMetadata.partition() + "\n" +
                            "Offset: "+ recordMetadata.offset() + "\n" +
                            "Timestamp: "+ recordMetadata.timestamp() + "\n"
                    );
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
