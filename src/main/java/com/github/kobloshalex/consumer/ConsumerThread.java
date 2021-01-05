package com.github.kobloshalex.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread {
  public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  public static final String GROUP_ID = "my-fourth-application";
  public static final String TOPIC = "second-topic";
  private static final Logger logger = LoggerFactory.getLogger((ConsumerThread.class));

  public static void main(String[] args) {
    new ConsumerThread().run();
  }

  private ConsumerThread() {}

  private void run() {
    CountDownLatch latch = new CountDownLatch(1);
    Runnable myConsumerThread = new ConsumerThreadHandler(latch);
    Thread thread = new Thread(myConsumerThread);
    thread.start();
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Cough shutdown hook");
                  ((ConsumerThreadHandler) myConsumerThread).shutDown();
                }));
    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.info("application fot interrupted");
    } finally {
      logger.info("application is closed");
    }
  }

  public class ConsumerThreadHandler implements Runnable {

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerThreadHandler(CountDownLatch latch) {
      this.latch = latch;

      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
      properties.setProperty(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      consumer = new KafkaConsumer<>(properties);
      consumer.subscribe(Collections.singleton(TOPIC));
    }

    @Override
    public void run() {
      try {
        while (true) {
          ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
          for (ConsumerRecord<String, String> record : consumerRecords) {
            logger.info("key: " + record.key() + " value: " + record.value());
            logger.info("partition: " + record.partition() + " offset: " + record.offset());
          }
        }
      } catch (WakeupException e) {
        logger.info("received shutdown signal");
      } finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutDown() {
      consumer.wakeup();
    }
  }
}
