package rusiashka.kafka.start.producer;

import rusiashka.kafka.start.producer.kafka_twitter.TwitterKafkaApp;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaTweeterProducerApplication implements CommandLineRunner {

    @Setter(onMethod=@__({@Autowired}))
    private TwitterKafkaApp twitterKafkaApp;

    public static void main(String[] args) {
        SpringApplication.run(KafkaTweeterProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        twitterKafkaApp.startApp();
    }
}
