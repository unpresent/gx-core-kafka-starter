package ru.gx.core.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.gx.core.kafka.listener.KafkaSimpleListener;
import ru.gx.core.kafka.listener.KafkaSimpleListenerSettingsContainer;
import ru.gx.core.kafka.load.AbstractKafkaIncomeTopicsConfiguration;
import ru.gx.core.kafka.load.KafkaIncomeTopicsLoader;
import ru.gx.core.kafka.load.KafkaIncomeTopicsOffsetsController;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.kafka.upload.KafkaOutcomeTopicsUploader;
import ru.gx.core.messaging.DefaultMessagesFactory;
import ru.gx.core.messaging.MessagesPrioritizedQueue;
import ru.gx.core.settings.StandardSettingsController;

import java.util.List;

@Configuration
@EnableConfigurationProperties({ConfigurationPropertiesServiceKafka.class})
public class CommonAutoConfiguration {
    private final static String DOT_ENABLED = ".enabled";
    private final static String DOT_NAME = ".name";

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

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            value = KafkaSimpleListenerSettingsContainer.SIMPLE_LISTENER_SETTINGS_PREFIX + DOT_ENABLED,
            havingValue = "true"
    )
    @Autowired
    public KafkaSimpleListenerSettingsContainer kafkaSimpleListenerSettingsContainer(
            @NotNull final StandardSettingsController standardSettingsController
    ) {
        return new KafkaSimpleListenerSettingsContainer(standardSettingsController);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            value = KafkaSimpleListenerSettingsContainer.SIMPLE_LISTENER_SETTINGS_PREFIX + DOT_ENABLED,
            havingValue = "true"
    )
    @Autowired
    public KafkaSimpleListener KafkaSimpleListener(
            @Value("${service.name}") @NotNull final String serviceName,
            @Value("${"
                    + KafkaSimpleListenerSettingsContainer.SIMPLE_LISTENER_SETTINGS_PREFIX + DOT_NAME
                    + ":" + KafkaSimpleListener.SIMPLE_LISTENER_DEFAULT_NAME + "}"
            ) final String name,
            @NotNull final KafkaSimpleListenerSettingsContainer settingsContainer,
            @NotNull final MeterRegistry meterRegistry,
            @NotNull final ApplicationEventPublisher eventPublisher,
            @NotNull final KafkaIncomeTopicsLoader kafkaIncomeTopicsLoader,
            @NotNull final List<AbstractKafkaIncomeTopicsConfiguration> kafkaIncomeTopicsConfigurations,
            @NotNull final TopicsOffsetsStorage topicsOffsetsStorage,
            @NotNull final KafkaIncomeTopicsOffsetsController kafkaIncomeTopicsOffsetsController
    ) {
        return new KafkaSimpleListener(
                serviceName,
                name,
                settingsContainer,
                meterRegistry,
                eventPublisher,
                kafkaIncomeTopicsLoader,
                kafkaIncomeTopicsConfigurations,
                topicsOffsetsStorage,
                kafkaIncomeTopicsOffsetsController
        );
    }
}
