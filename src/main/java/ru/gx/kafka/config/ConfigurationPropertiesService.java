package ru.gx.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "service")
@Getter
@Setter
public class ConfigurationPropertiesService {
    @NestedConfigurationProperty
    private IncomeTopics incomeTopics;

    @NestedConfigurationProperty
    private OutcomeTopics outcomeTopics;

    @Getter
    @Setter
    public static class IncomeTopics {
        @NestedConfigurationProperty
        private ConfiguratorCaller configuratorCaller = new ConfiguratorCaller();

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
        private ConfiguratorCaller configuratorCaller = new ConfiguratorCaller();

        @NestedConfigurationProperty
        private SimpleConfiguration simpleConfiguration = new SimpleConfiguration();

        @NestedConfigurationProperty
        private StandardUploader standardUploader = new StandardUploader();

        @NestedConfigurationProperty
        private StandardOffsetsController standardOffsetsController = new StandardOffsetsController();
    }

    @Getter
    @Setter
    public static class ConfiguratorCaller {
        private boolean enabled = true;
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
