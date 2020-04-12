package rusiashka.kafka.start.consumer.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rusiashka.kafka.start.consumer.config.KafkaConsumerConfig;

import java.util.Arrays;
import java.util.Properties;

@Component
public class TwitterKafkaConsumer {
    @Autowired
    KafkaConsumerConfig config;
    public KafkaConsumer<String, String> createKafkaConsumer() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBoostrapServer());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializer());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset());

        KafkaConsumer kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(config.getTopic()));
        return kafkaConsumer;
    }
}
