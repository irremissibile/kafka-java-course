package co.winish.kafka.demo;

import co.winish.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKeysDemo {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerKeysDemo.class);

        KafkaProducer<String, String> producer = KafkaConfig.getStringKafkaProducer();
        String topic = "demo";

        for (int i = 0; i < 10; i++) {
            String key = "id_" + i;
            String value = Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    logger.info("Received new metadata: \n" +
                            "Topic: "       + recordMetadata.topic() + "\n" +
                            "Partition: "   + recordMetadata.partition() + "\n" +
                            "Offset: "      + recordMetadata.offset() + "\n" +
                            "Timestamp: "   + recordMetadata.timestamp());
                } else
                    logger.error("Error while producing", e);
            });
        }

        producer.flush();
        producer.close();
    }
}
