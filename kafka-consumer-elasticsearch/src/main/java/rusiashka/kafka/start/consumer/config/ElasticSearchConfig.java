package rusiashka.kafka.start.consumer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "elastic.search.config")
public class ElasticSearchConfig {
    private String hostname;
    private Integer port;
    private String scheme;
    private String username;
    private String password;
}
