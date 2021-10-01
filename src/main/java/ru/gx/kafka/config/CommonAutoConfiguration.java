package ru.gx.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.kafka.load.IncomeTopicsConfiguratorCaller;
import ru.gx.kafka.load.SimpleIncomeTopicsLoader;
import ru.gx.kafka.upload.OutcomeTopicsConfiguratorCaller;
import ru.gx.kafka.upload.SimpleOutcomeTopicUploader;

@Configuration
public class CommonAutoConfiguration {
    @Value("${service.name}")
    private String serviceName;

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.simple-loader.enabled", havingValue = "true")
    public SimpleIncomeTopicsLoader simpleIncomeTopicsLoader() {
        return new SimpleIncomeTopicsLoader(this.serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.income-topics.configurator-caller.enabled", havingValue = "true")
    public IncomeTopicsConfiguratorCaller incomeTopicsConfiguratorCaller() {
        return new IncomeTopicsConfiguratorCaller();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-topics.simple-uploader.enabled", havingValue = "true")
    public SimpleOutcomeTopicUploader simpleOutcomeTopicUploader() {
        return new SimpleOutcomeTopicUploader();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.outcome-topics.configurator-caller.enabled", havingValue = "true")
    public OutcomeTopicsConfiguratorCaller outcomeTopicsConfiguratorCaller() {
        return new OutcomeTopicsConfiguratorCaller();
    }
}
