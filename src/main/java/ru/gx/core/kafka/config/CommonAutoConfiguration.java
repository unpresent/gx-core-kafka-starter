package ru.gx.core.kafka.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.core.kafka.load.KafkaIncomeTopicsLoader;
import ru.gx.core.kafka.load.KafkaIncomeTopicsOffsetsController;
import ru.gx.core.kafka.upload.KafkaOutcomeTopicsUploader;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesServiceKafka.class})
public class CommonAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.kafka.income-topics.standard-loader.enabled", havingValue = "true")
    public KafkaIncomeTopicsLoader standardIncomeTopicsLoader() {
        return new KafkaIncomeTopicsLoader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.kafka.income-topics.standard-offsets-controller.enabled", havingValue = "true")
    public KafkaIncomeTopicsOffsetsController standardIncomeTopicsOffsetsController() {
        return new KafkaIncomeTopicsOffsetsController();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.kafka.outcome-topics.standard-uploader.enabled", havingValue = "true")
    public KafkaOutcomeTopicsUploader standardOutcomeTopicsUploader() {
        return new KafkaOutcomeTopicsUploader();
    }
}
