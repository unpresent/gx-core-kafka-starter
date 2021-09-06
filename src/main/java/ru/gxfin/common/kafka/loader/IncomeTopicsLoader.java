package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.data.ObjectAlreadyExistsException;
import ru.gxfin.common.data.ObjectNotExistsException;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

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
     * @return                                  Список загруженных объектов.
     */
    <O extends DataObject, P extends DataPackage<O>> Collection<O> processByTopic(IncomeTopicLoadingDescriptor<O, P> descriptor, Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException;

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     * @param durationOnPoll                    Длительность, в течение которой ожидать данных из Топика.
     * @throws JsonProcessingException          Ошибки при десериализации из Json-а.
     * @return                                  Map-а, в которой для каждого дескриптора указан список загруженных объектов.
     */
    Map<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>, Collection<DataObject>> processAllTopics(Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException;
}
