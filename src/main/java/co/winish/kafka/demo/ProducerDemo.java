package co.winish.kafka.demo;

import co.winish.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = KafkaConfig.getStringKafkaProducer();

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_topic", "salam alejkym");

        producer.send(record);

        producer.flush();
        producer.close();
    }
}
