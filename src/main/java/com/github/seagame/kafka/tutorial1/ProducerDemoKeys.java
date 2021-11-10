package com.github.seagame.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException,InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";
        System.out.println("Create Producer Properties");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.out.println("Create The Producer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        System.out.println("Send data");
        for (int i=0; i<10; i++) {
            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("Key: "+ key);
            // id_0 partition 1
            // id 1 - 0
            // id 2 - 2
            // id 3 - 0


            // create Producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

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
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
