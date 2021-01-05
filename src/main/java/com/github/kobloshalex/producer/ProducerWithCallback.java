package com.github.kobloshalex.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {
  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  public static final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

  public static void main(String[] args) {
    // create Producer props
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create produces
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {
      //

      // create record
      ProducerRecord<String, String> producerRecord =
          new ProducerRecord<String, String>("first-topic", "hello world " + i);

      // send data
      producer.send(
          producerRecord,
          new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // executes every time record is sends or exception
              if (e == null) {
                System.out.println(("Received new metadata" + recordMetadata.topic()));
                logger.info("Received new partition" + recordMetadata.partition());
                logger.info("Received new offset" + recordMetadata.offset());
                logger.info("Received new timestamp" + recordMetadata.timestamp());
              } else {
                logger.error(String.valueOf(e));
              }
            }
          });
    }

    producer.flush();
    producer.close();
  }
}
