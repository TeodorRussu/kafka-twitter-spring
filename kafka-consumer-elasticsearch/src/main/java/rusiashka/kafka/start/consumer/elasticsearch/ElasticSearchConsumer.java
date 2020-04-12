package rusiashka.kafka.start.consumer.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rusiashka.kafka.start.consumer.config.ElasticSearchConfig;
import rusiashka.kafka.start.consumer.kafka_consumer.TwitterKafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class ElasticSearchConsumer {

    @Autowired
    private TwitterKafkaConsumer twitterKafkaConsumer;
    @Autowired
    private ElasticSearchConfig elasticSearchConfig;

    public RestHighLevelClient createClient() {
        final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticSearchConfig.getUsername(), elasticSearchConfig.getPassword()));

        RestClientBuilder restHighLevelClient = RestClient.builder(new HttpHost(elasticSearchConfig.getHostname(), elasticSearchConfig.getPort(), elasticSearchConfig.getScheme()))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider)
                );

        return new RestHighLevelClient(restHighLevelClient);
    }

    public void action() {
        RestHighLevelClient restHighLevelClient = createClient();
        try (KafkaConsumer<String, String> kafkaConsumer = twitterKafkaConsumer.createKafkaConsumer()) {
            AtomicBoolean isAlive = new AtomicBoolean(true);
            while (isAlive.get()) {
                try {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(5000));
                    records.forEach(record -> {
                        IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                                .source(record.value(), XContentType.JSON);

                        IndexResponse indexResponse;
                        try {
                            indexResponse = restHighLevelClient.index(indexRequest);
                            String id = indexResponse.getId();
                            log.info(id);

                            if (id.equals("rtqwwevshsteesnshddsfd")) {
                                isAlive.set(false);
                            }
                        } catch (IOException e) {
                            log.error(e.getMessage());
                        }
                    });
                } catch (Exception exception) {
                    log.error(exception.getMessage());
                }
            }
        }
    }


}
