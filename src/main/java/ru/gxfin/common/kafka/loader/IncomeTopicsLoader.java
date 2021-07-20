package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.configuration.IncomeTopicsConfiguration;

import java.time.Duration;

/**
 * Интерфейс вспомогательного загрузчика, которые упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@SuppressWarnings("unused")
public interface IncomeTopicsLoader {
    /**
     * Чтение набора DataPackage-ей из очереди.
     * @param topic2MemRepo Описатель обработчика одной очереди.
     * @param durationOnPoll Длительность ожидания данных в очереди.
     * @return Набор DataPackage-ей из очереди.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    @SuppressWarnings("rawtypes")
    Iterable<DataPackage> loadPackages(IncomeTopicLoadingDescriptor topic2MemRepo, Duration durationOnPoll) throws JsonProcessingException;

    /**
     * Чтение набора DataObject-ов из очереди.
     * @param topic2MemRepo Описатель обработчика одной очереди.
     * @param durationOnPoll Длительность ожидания данных в очереди.
     * @return Набор DataObject-ов из очереди.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    Iterable<DataObject> loadObjects(IncomeTopicLoadingDescriptor topic2MemRepo, Duration durationOnPoll) throws JsonProcessingException;

    void loadTopicsByConfiguration(IncomeTopicsConfiguration configuration, Duration durationOnPoll) throws JsonProcessingException;
}
