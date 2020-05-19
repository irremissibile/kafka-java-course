package co.winish.kafka.demo;

import co.winish.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        KafkaConsumer<String, String> consumer = KafkaConfig.getStringKafkaConsumer("consumer_demo");

        String topic = "demo";
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> logger.info("Key: " + record.key() + ", Value: " + record.value() + ", Partition: " + record.partition()));
        }
    }
}
