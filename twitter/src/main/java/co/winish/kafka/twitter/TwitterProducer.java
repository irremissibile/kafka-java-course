package co.winish.kafka.twitter;

import co.winish.kafka.config.KafkaConfig;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");
    private String kafkaTopic = "twitter";


    public static void main(String[] args) {
        new TwitterProducer().run();
    }


    private void run() {
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>(1000);
        Client client = initTwitterClient(messageQueue);
        client.connect();

        KafkaProducer<String, String> producer = KafkaConfig.getBatchStringProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping twitter client");
            client.stop();
            logger.info("Closing Kafka producer");
            producer.close();
            logger.info("Everything's stopped");
        }));


        while (!client.isDone()) {
            String message = null;

            try {
                message = messageQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (message != null){
                logger.info(message);
                producer.send(new ProducerRecord<>(kafkaTopic, message), (recordMetadata, e) -> {
                    if (e != null)
                        logger.error(e.getMessage());
                });
            }
        }

        logger.info("End of an application");
    }



    private Client initTwitterClient(BlockingQueue<String> messageQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        Properties twitterProperties = readTwitterPropetriesFile();

        String consumerKey = twitterProperties.getProperty("consumer_api_key");
        String consumerSecret = twitterProperties.getProperty("consumer_api_key_secret");
        String accessKey = twitterProperties.getProperty("access_token");
        String accessSecret = twitterProperties.getProperty("access_token_secret");

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, accessKey, accessSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(messageQueue));

        return builder.build();
    }


    private Properties readTwitterPropetriesFile() {
        Properties properties = new Properties();

        try (InputStream input = TwitterProducer.class.getClassLoader().getResourceAsStream("twitter.properties")) {
            properties.load(input);
        } catch (Exception e) {
            logger.error("Twitter.properties file not found");
            e.printStackTrace();
        }

        return properties;
    }
}
