package ru.gx.core.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.core.kafka.load.KafkaIncomeTopicsLoader;
import ru.gx.core.kafka.load.KafkaIncomeTopicsOffsetsController;
import ru.gx.core.kafka.upload.KafkaOutcomeTopicsUploader;
import ru.gx.core.messaging.DefaultMessagesFactory;
import ru.gx.core.messaging.MessagesPrioritizedQueue;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesServiceKafka.class})
public class CommonAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(value = "service.kafka.income-topics.standard-loader.enabled", havingValue = "true")
    @Autowired
    public KafkaIncomeTopicsLoader standardIncomeTopicsLoader(
            @NotNull final ApplicationEventPublisher eventPublisher,
            @NotNull final ObjectMapper objectMapper,
            @NotNull final MessagesPrioritizedQueue messagesQueue
    ) {
        return new KafkaIncomeTopicsLoader(eventPublisher, objectMapper, messagesQueue);
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
    @Autowired
    public KafkaOutcomeTopicsUploader standardOutcomeTopicsUploader(
            @NotNull final ObjectMapper objectMapper,
            @NotNull final DefaultMessagesFactory messagesFactory
    ) {
        return new KafkaOutcomeTopicsUploader(objectMapper, messagesFactory);
    }
}
