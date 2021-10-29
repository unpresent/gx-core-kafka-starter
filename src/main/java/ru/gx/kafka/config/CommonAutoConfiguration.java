package ru.gx.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.kafka.load.*;
import ru.gx.kafka.upload.OutcomeTopicsConfiguratorCaller;
import ru.gx.kafka.upload.SimpleOutcomeTopicsConfiguration;
import ru.gx.kafka.upload.StandardOutcomeTopicsUploader;

@Configuration
public class CommonAutoConfiguration {
    @Value("${service.name}")
    private String serviceName;

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.simple-configuration.enabled", havingValue = "true")
    public SimpleIncomeTopicsConfiguration simpleIncomeTopicsConfiguration() {
        return new SimpleIncomeTopicsConfiguration(serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.standard-loader.enabled", havingValue = "true")
    public StandardIncomeTopicsLoader standardIncomeTopicsLoader() {
        return new StandardIncomeTopicsLoader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.standard-offsets-controller.enabled", havingValue = "true")
    public StandardIncomeTopicsOffsetsController standardIncomeTopicsOffsetsController() {
        return new StandardIncomeTopicsOffsetsController();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.configurator-caller.enabled", havingValue = "true")
    public IncomeTopicsConfiguratorCaller incomeTopicsConfiguratorCaller() {
        return new IncomeTopicsConfiguratorCaller();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-topics.simple-configuration.enabled", havingValue = "true")
    public SimpleOutcomeTopicsConfiguration simpleOutcomeTopicsConfiguration() {
        return new SimpleOutcomeTopicsConfiguration();
    }

    @Bean
    @ConditionalOnMissingBean
    public IncomeTopicsLoadingDescriptorsFactory incomeTopicsLoadingDescriptorsFactory() {
        return new StandardIncomeTopicsLoadingDescriptorsFactory();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-topics.standard-uploader.enabled", havingValue = "true")
    public StandardOutcomeTopicsUploader standardOutcomeTopicsUploader() {
        return new StandardOutcomeTopicsUploader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-topics.configurator-caller.enabled", havingValue = "true")
    public OutcomeTopicsConfiguratorCaller outcomeTopicsConfiguratorCaller() {
        return new OutcomeTopicsConfiguratorCaller();
    }
}
