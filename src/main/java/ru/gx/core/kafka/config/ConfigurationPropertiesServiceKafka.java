package ru.gx.core.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "service.kafka")
@Getter
@Setter
public class ConfigurationPropertiesServiceKafka {
    private String server;

    @NestedConfigurationProperty
    private IncomeTopics incomeTopics;

    @NestedConfigurationProperty
    private OutcomeTopics outcomeTopics;

    @Getter
    @Setter
    public static class IncomeTopics {
        @NestedConfigurationProperty
        private SimpleConfiguration simpleConfiguration = new SimpleConfiguration();

        @NestedConfigurationProperty
        private StandardLoader standardLoader = new StandardLoader();

        @NestedConfigurationProperty
        private StandardOffsetsController standardOffsetsController = new StandardOffsetsController();
    }

    @Getter
    @Setter
    public static class OutcomeTopics {
        @NestedConfigurationProperty
        private SimpleConfiguration simpleConfiguration = new SimpleConfiguration();

        @NestedConfigurationProperty
        private StandardUploader standardUploader = new StandardUploader();
    }

    @Getter
    @Setter
    public static class SimpleConfiguration {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class StandardOffsetsController {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class StandardLoader {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class StandardUploader {
        private boolean enabled = true;
    }
}