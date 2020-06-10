package com.vijay.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.tools.jconsole.JConsole;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

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
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        /* subscribe consumer to our topics */
        consumer.subscribe(Collections.singleton(topic));

        /* Poll for new data */
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {
                logger.info("Key: " + record.key() + ",Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }
}
