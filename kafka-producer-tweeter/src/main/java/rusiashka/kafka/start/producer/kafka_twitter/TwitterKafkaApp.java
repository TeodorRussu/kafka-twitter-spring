package rusiashka.kafka.start.producer.kafka_twitter;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TwitterKafkaApp {

    @Autowired
    private TwitterProducer twitterProducer;

    @Value("#{'${twitter.filtering.keywords}'.split(',')}")
    List<String> trackedKeywords;

    public void startApp() throws InterruptedException {
        twitterProducer.setTrackedKeywords(trackedKeywords);
        twitterProducer.clientAction();
    }
}
