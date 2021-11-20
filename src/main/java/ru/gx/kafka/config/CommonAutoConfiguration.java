package ru.gx.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.kafka.load.KafkaIncomeTopicsLoader;
import ru.gx.kafka.load.KafkaIncomeTopicsOffsetsController;
import ru.gx.kafka.load.SimpleKafkaIncomeTopicsConfiguration;
import ru.gx.kafka.upload.KafkaOutcomeTopicsUploader;
import ru.gx.kafka.upload.SimpleKafkaOutcomeTopicsConfiguration;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesService.class, ConfigurationPropertiesKafka.class})
public class CommonAutoConfiguration {
    private static final String SIMPLE_INCOME_CONFIG_PREFIX = ":in:simple-kafka";
    private static final String SIMPLE_OUTCOME_CONFIG_PREFIX = ":out:simple-kafka";

    @Value("${service.name}")
    private String serviceName;

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.simple-configuration.enabled", havingValue = "true")
    public SimpleKafkaIncomeTopicsConfiguration simpleIncomeTopicsConfiguration() {
        return new SimpleKafkaIncomeTopicsConfiguration(this.serviceName + SIMPLE_INCOME_CONFIG_PREFIX);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.standard-loader.enabled", havingValue = "true")
    public KafkaIncomeTopicsLoader standardIncomeTopicsLoader() {
        return new KafkaIncomeTopicsLoader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.standard-offsets-controller.enabled", havingValue = "true")
    public KafkaIncomeTopicsOffsetsController standardIncomeTopicsOffsetsController() {
        return new KafkaIncomeTopicsOffsetsController();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-topics.simple-configuration.enabled", havingValue = "true")
    public SimpleKafkaOutcomeTopicsConfiguration simpleKafkaOutcomeTopicsConfiguration() {
        return new SimpleKafkaOutcomeTopicsConfiguration(this.serviceName + SIMPLE_OUTCOME_CONFIG_PREFIX);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-topics.standard-uploader.enabled", havingValue = "true")
    public KafkaOutcomeTopicsUploader standardOutcomeTopicsUploader() {
        return new KafkaOutcomeTopicsUploader();
    }
}
