package ru.gx.kafka.events;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.load.*;

import java.util.Collection;

/**
 * Интерфейс объектов-событий, которые бросаются после загрузки объектов.
 * Если в загрузке участвует репозиторий, то после сохранения в репозиторий.
 */
@SuppressWarnings("unused")
public interface OnRawDataLoadedFromIncomeTopicEvent {

    /**
     * @return Получение описателя загрузки из Топика.
     */
    @NotNull
    RawDataIncomeTopicLoadingDescriptor getLoadingDescriptor();

    /**
     * @return Список объектов, которые были загружены.
     */
    @NotNull
    Collection<ConsumerRecord<?, ?>> getData();

    /**
     * @return Режим продолжения обработки других Топиков.
     */
    @NotNull
    IncomeTopicsLoaderContinueMode getContinueMode();

    /**
     * Установка начальных значений перед "бросанием" события.
     * @param source                Источник события.
     * @param loadingDescriptor     Описатель загрузки из Топика.
     * @param records               Список прочитанных записей.
     * @return                      this.
     */
    @SuppressWarnings("UnusedReturnValue")
    @NotNull
    OnRawDataLoadedFromIncomeTopicEvent reset(@NotNull final Object source, @NotNull final RawDataIncomeTopicLoadingDescriptor loadingDescriptor, @NotNull final IncomeTopicsLoaderContinueMode continueMode, @NotNull final Collection<ConsumerRecord<?, ?>> records);
}
