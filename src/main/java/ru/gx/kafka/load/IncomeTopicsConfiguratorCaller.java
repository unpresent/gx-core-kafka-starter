package ru.gx.kafka.load;

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
public class IncomeTopicsConfiguratorCaller {
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private IncomeTopicsConfigurator incomeTopicsConfigurator;

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private Collection<IncomeTopicsConfiguration> configurations;

    /**
     * Обработчик события о том, что все бины построены. Приложение готово к запуску.
     * Вызываем конфигураторы настройки обработчиков входящих потоков.
     */
    @SuppressWarnings("unused")
    @EventListener(ApplicationReadyEvent.class)
    @ConditionalOnProperty(value = "service.income-topics.configurator-caller.enabled", havingValue = "true")
    public void onApplicationApplicationReady(ApplicationReadyEvent __) {
        if (this.incomeTopicsConfigurator == null) {
            throw new BeanInitializationException("Not initialized bean IncomeTopicsConfigurator!");
        }
        if (this.configurations == null) {
            throw new BeanInitializationException("Not initialized bean Collection<IncomeTopicsConfiguration>!");
        }
        this.configurations.forEach(c -> {
            log.info("Starting configure IncomeTopicsConfiguration: {}", c);
            this.incomeTopicsConfigurator.configureIncomeTopics(c);
            log.info("Finished configure IncomeTopicsConfiguration: {}", c);
        });
    }
}
