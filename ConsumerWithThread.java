package com.vijay.kafka;

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

public class ConsumerWithThread {

    public static void main(String[] args) {
        new ConsumerWithThread().run();
    }

    private ConsumerWithThread() {

    }

    private void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create a consumer runnable
        logger.info("Creating a consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }
        finally {
            logger.info("Application is closing");
        }
    }


    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) { // for concurrency CountDownLatch
            this.latch = latch;

            /* Create consumer configs */

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            // When Producer receive the string message and convert it into bytes and send to kafka
            // Than Consumer receive the byte message and convert it to the string
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // earliest read the offset from the starting
            // latest read the offset from the end
            // none error if not offset


            /* Create consumer */
            consumer = new KafkaConsumer<String, String>(properties);

            /* subscribe consumer to our topics */
            consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {
            /* Poll for new data */
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ",Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            }
            catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            }
            finally {
                consumer.close();

                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            // the wakeup() Method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
