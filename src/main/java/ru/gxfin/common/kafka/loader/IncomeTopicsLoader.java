package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import ru.gxfin.common.data.ObjectAlreadyExistsException;
import ru.gxfin.common.data.ObjectNotExistsException;
import ru.gxfin.common.kafka.configuration.IncomeTopicsConfiguration;

import java.time.Duration;

/**
 * Интерфейс вспомогательного загрузчика, которые упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@SuppressWarnings("unused")
public interface IncomeTopicsLoader {
    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     * @param descriptor                        Описатель загрузки из Топика.
     * @param durationOnPoll                    Длительность, в течение которой ожидать данных из Топика.
     * @throws JsonProcessingException          Ошибки при десериализации из Json-а.
     */
    @SuppressWarnings("rawtypes")
    int processByTopic(IncomeTopicLoadingDescriptor descriptor, Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException;

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     * @param configuration                     Конфигурация.
     * @param durationOnPoll                    Длительность, в течение которой ожидать данных из Топика.
     * @throws JsonProcessingException          Ошибки при десериализации из Json-а.
     */
    void processTopicsByConfiguration(IncomeTopicsConfiguration configuration, Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException;
}
