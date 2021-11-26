package ru.gx.core.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "kafka")
@Getter
@Setter
public class ConfigurationPropertiesKafka {
    private String server;
}
