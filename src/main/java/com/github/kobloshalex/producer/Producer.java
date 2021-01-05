package com.github.kobloshalex.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

  public static void main(String[] args) {
    // create Producer props
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create produces
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // create record
    ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("first-topic", "hello world");

    // send data
    producer.send(producerRecord);
    producer.close();
  }
}
