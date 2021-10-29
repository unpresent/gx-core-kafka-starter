package ru.gx.kafka.upload;

import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.load.IncomeTopicsConfiguration;

/**
 * Реализатор данного интерфейса будет вызван после настройки всех бинов (во время обработки ApplicationReadyEvent).
 * Задача реализатора данного интерфейса заключается в конфигурировании исходящих потоков в Kafka.
 */
@SuppressWarnings("unused")
public interface OutcomeTopicsConfigurator {
    /**
     * Вызывается после настройки бинов (в BeanPostProcessor-е).
     * @param configurator Передается бин, реализующий интерфейс OutcomeTopicsConfiguration. Данный бин в методе реализации требуется настроить.
     */
    void configureOutcomeTopics(@NotNull final OutcomeTopicsConfiguration configurator);
}
