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
     * @param uploader Передается бин, реализующий интерфейс OutcomeTopicsConfigurator. Данный бин в методе реализации требуется настроить.
     */
    void configureOutcomeTopics(@NotNull final OutcomeTopicUploader uploader);
}
