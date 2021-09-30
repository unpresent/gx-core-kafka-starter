package ru.gx.kafka.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.jetbrains.annotations.NotNull;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.data.ObjectAlreadyExistsException;
import ru.gx.data.ObjectNotExistsException;

import java.security.InvalidParameterException;
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
    @NotNull
    <O extends DataObject, P extends DataPackage<O>> Collection<O> processByTopic(@NotNull final IncomeTopicLoadingDescriptor<O, P> descriptor, @NotNull final Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException;

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     * @param durationOnPoll                    Длительность, в течение которой ожидать данных из Топика.
     * @throws JsonProcessingException          Ошибки при десериализации из Json-а.
     * @return                                  Map-а, в которой для каждого дескриптора указан список загруженных объектов.
     */
    @NotNull
    Map<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>, Collection<DataObject>> processAllTopics(@NotNull Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException, InvalidParameterException;
}
