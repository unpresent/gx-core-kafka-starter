package ru.gx.kafka.upload;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

/**
 * Задача данного Bean-а вызвать настройщиков конфигураций обработки входящих потоков.
 */
@Slf4j
public class OutcomeTopicsConfiguratorCaller {
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private OutcomeTopicsConfigurator outcomeTopicsConfigurator;

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private Collection<OutcomeTopicsConfiguration> configurations;

    /**
     * Обработчик события о том, что все бины построены. Приложение готово к запуску.
     * Вызываем конфигураторы настройки обработчиков исходящих потоков.
     */
    @EventListener(ApplicationReadyEvent.class)
    @ConditionalOnProperty(value = "service.outcome-topics.configurator-caller.enabled", havingValue = "true")
    public void onApplicationApplicationReady(ApplicationReadyEvent __) {
        if (this.outcomeTopicsConfigurator == null) {
            throw new BeanInitializationException("Not initialized bean OutcomeTopicsConfigurator!");
        }
        if (this.configurations == null) {
            throw new BeanInitializationException("Not initialized bean Collection<OutcomeTopicsConfiguration>!");
        }
        this.configurations.forEach(configuration -> {
            log.info("Starting configure OutcomeTopicUploader: {}", configuration);
            this.outcomeTopicsConfigurator.configureOutcomeTopics(configuration);
            log.info("Finished configure OutcomeTopicUploader: {}", configuration);
        });
    }
}
