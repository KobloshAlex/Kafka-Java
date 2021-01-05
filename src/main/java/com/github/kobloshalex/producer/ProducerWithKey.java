package com.github.kobloshalex.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKey {
  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  public static final Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    // create Producer props
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create produces
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 0; i < 10; i++) {
      final String topic = "second-topic";
      final String value = "hello world";
      final String key = "id_ " + i;
      // create record
      ProducerRecord<String, String> producerRecord =
              new ProducerRecord<>(topic, key, value);

      // send data
      producer
          .send(
              producerRecord,
              (recordMetadata, exception) -> {
                // executes every time record is sends or exception
                if (exception == null) {
                  logger.info("Received new metadata: " + recordMetadata.topic());
                  logger.info("Received new partition: " + recordMetadata.partition());
                  logger.info("Received new offset: " + recordMetadata.offset());
                  logger.info("Received new timestamp: " + recordMetadata.timestamp());
                } else {
                  logger.error(String.valueOf(exception));
                }
              })
          .get();
    }

    producer.flush();
    producer.close();
  }
}
