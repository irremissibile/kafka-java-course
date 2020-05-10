package co.winish.kafka.demo;

import co.winish.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerCallbackDemo {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);

        KafkaProducer<String, String> producer = KafkaConfig.getStringKafkaProducer();


        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_topic", Integer.toString(80 + i));

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata: \n" +
                                "Topic: "       + recordMetadata.topic() + "\n" +
                                "Partition: "   + recordMetadata.partition() + "\n" +
                                "Offset: "      + recordMetadata.offset() + "\n" +
                                "Timestamp: "   + recordMetadata.timestamp());
                    } else
                        logger.error("Error while producing", e);
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
