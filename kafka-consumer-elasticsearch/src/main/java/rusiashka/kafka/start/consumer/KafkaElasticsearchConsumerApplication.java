package rusiashka.kafka.start.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import rusiashka.kafka.start.consumer.elasticsearch.ElasticSearchConsumer;

@SpringBootApplication
public class KafkaElasticsearchConsumerApplication implements CommandLineRunner {

    @Autowired
    private ElasticSearchConsumer elasticSearchConsumer;

    public static void main(String[] args) {
        SpringApplication.run(KafkaElasticsearchConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) {
        elasticSearchConsumer.action();
    }

}