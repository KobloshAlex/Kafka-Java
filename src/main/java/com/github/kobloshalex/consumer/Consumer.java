package com.github.kobloshalex.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  public static final String GROUP_ID = "my-third-application";
  public static final String TOPIC = "second-topic";

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger((Consumer.class));

    // create consumer setting
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // subscribe consumer
    consumer.subscribe(Collections.singleton(TOPIC));
    // poll for new data
    while (true) {
      ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, String> record : consumerRecords) {
        logger.info("key: " + record.key() + " value: " + record.value());
        logger.info("partition: " + record.partition() + " offset: " + record.offset());
      }
    }
  }
}
