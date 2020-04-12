package rusiashka.kafka.start.producer.kafka_twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
@Data
@Component
public class TwitterProducer {

    @Setter(onMethod=@__({@Autowired}))
    Environment environment;

    @Value("${apiKey}")
    private String apiKey;
    @Value("${apiSecret}")
    private String apiSecret;
    @Value("${token}")
    private String token;
    @Value("${tokenSecret}")
    private String tokenSecret;

    List<String> trackedKeywords;

    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
     */
    private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);


    public Client crateTwitterClient() {

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(trackedKeywords);


        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(apiKey, apiSecret, token, tokenSecret);


        ClientBuilder clientBuilder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        return clientBuilder.build();
    }

    public void clientAction() throws InterruptedException {
        Client twitterClient = crateTwitterClient();
        KafkaProducer <String, String> producer = createKafkaProducer();
        Callback callback = (RecordMetadata recordMetadata, Exception exception) -> {
            if (exception != null) {
                log.error("Error occured: " + exception);
            }
        };

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            twitterClient.stop();
            log.info("twitter client stopped");
            producer.close();
            log.info("kafka producer stopped");
        }));

        // Attempts to establish a connection.
        twitterClient.connect();

        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = msgQueue.take();
            log.info(msg);
            if(msg.contains("text")){
                producer.send(new ProducerRecord<>("twitter_tweets", "tweet_key", msg), callback);
            }
        }
        twitterClient.stop();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("bootstrap.servers"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, environment.getProperty("key.serializer"));
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, environment.getProperty("value.serializer"));

        //safe producer properties
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, environment.getProperty("enable.idempotence"));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, environment.getProperty("acks"));
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, environment.getProperty("retries"));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, environment.getProperty("max.in.flight.requests.per.connection")); //5

        //high throughput producer(at the expence of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, environment.getProperty("compression.type"));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, environment.getProperty("linger.ms"));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, environment.getProperty("batch.size"));

        return new KafkaProducer<>(properties);
    }
}
