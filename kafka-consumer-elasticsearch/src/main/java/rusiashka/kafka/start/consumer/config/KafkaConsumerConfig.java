package rusiashka.kafka.start.consumer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "kafka.consumer.config")
public class KafkaConsumerConfig {
    String boostrapServer;
    String groupId;
    String topic;
    String autoOffsetReset;
    String keyDeserializer;
    String valueDeserializer;
}
