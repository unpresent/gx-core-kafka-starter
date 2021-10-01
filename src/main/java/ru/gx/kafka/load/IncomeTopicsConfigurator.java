package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;

/**
 * Реализатор данного интерфейса будет вызван после настройки всех бинов.
 * Задача реализатора данного интерфейса заключается в конфигурировании входящих потоков из Kafka.
 */
@SuppressWarnings("unused")
public interface IncomeTopicsConfigurator {
    /**
     * Вызывается после настройки бинов (в BeanPostProcessor-е).
     * @param configuration Передается бин, реализующий интерфейс IncomeTopicsConfiguration. Данный бин в методе реализации требуется настроить.
     */
    void configureIncomeTopics(@NotNull final IncomeTopicsConfiguration configuration);
}
