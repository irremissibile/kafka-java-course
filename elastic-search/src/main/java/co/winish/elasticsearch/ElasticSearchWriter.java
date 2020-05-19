package co.winish.elasticsearch;

import co.winish.kafka.config.KafkaConfig;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ElasticSearchWriter {
    private Logger logger = LoggerFactory.getLogger(ElasticSearchWriter.class.getName());


    public static void main(String[] args) throws IOException {
        new ElasticSearchWriter().run();
    }


    private void run() {
        RestHighLevelClient client = initClient();
        String topic = "twitter";
        KafkaConsumer<String, String> consumer = KafkaConfig.getStringExplicitCommitKafkaConsumer("elasticsearch-writer");
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer thread");
        ElasticSearchConsumerRunnable myConsumerRunnable = new ElasticSearchConsumerRunnable(client, consumer, topic, latch);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }


    public RestHighLevelClient initClient(){
        Properties properties = readElasticSearchPropetriesFile();
        String hostname = properties.getProperty("hostname");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");


        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient
                .builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }


    private Properties readElasticSearchPropetriesFile() {
        Properties properties = new Properties();

        try (InputStream input = ElasticSearchWriter.class.getClassLoader().getResourceAsStream("elasticsearch.properties")) {
            properties.load(input);
        } catch (Exception e) {
            logger.error("elasticsearch.properties file not found");
            e.printStackTrace();
        }

        return properties;
    }
}


class ElasticSearchConsumerRunnable implements Runnable {
    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;
    private final RestHighLevelClient client;
    private final JsonParser jsonParser = new JsonParser();
    private final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerRunnable.class.getName());


    public ElasticSearchConsumerRunnable(RestHighLevelClient client, KafkaConsumer<String, String> consumer, String topic, CountDownLatch latch) {
        this.client = client;
        this.latch = latch;
        this.consumer = consumer;
        consumer.subscribe(Collections.singletonList(topic));
    }


    @Override
    public void run() {
        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                int recordCount = records.count();
                logger.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records){
                    try {
                        String id = extractTweetId(record.value());

                        IndexRequest indexRequest = new IndexRequest("tweets")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        bulkRequest.add(indexRequest);
                    } catch (NullPointerException e){
                        logger.warn("Something bad happened with: " + record.value());
                    }

                }

                if (recordCount > 0) {
                    try {
                        BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                        logger.info("Committing offsets...");
                        consumer.commitSync();
                        logger.info("Offsets have been committed");

                    } catch (IOException e) {
                        logger.error(e.getMessage());
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        logger.error(e.getMessage());
                    }
                }
            }



        } catch (WakeupException e) {
            logger.info("Received shutdown signal!");
        } finally {
            consumer.close();

            try {
                client.close();
            } catch (IOException e) {
                logger.error("Couldn't close the client");
                logger.error(e.getMessage());
            }

            latch.countDown();
        }
    }


    private String extractTweetId(String tweetJson){
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }


    public void shutdown() {
        consumer.wakeup();
    }
}