package ru.gx.kafka.events;

import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.IncomeTopicsLoaderContinueMode;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.loader.IncomeTopicLoadingDescriptor;

import java.util.Collection;

/**
 * Интерфейс объектов-событий, которые бросаются после загрузки объектов.
 * Если в загрузке участвует репозиторий, то после сохранения в репозиторий.
 * @param <O>   Класс загружаемых объектов.
 * @param <P>   Класс пакетов загружаемых объектов.
 */
@SuppressWarnings("unused")
public interface OnObjectsLoadedFromIncomeTopicEvent<O extends DataObject, P extends DataPackage<O>> {

    /**
     * @return Получение описателя загрузки из Топика.
     */
    @NotNull
    IncomeTopicLoadingDescriptor<O, P> getLoadingDescriptor();

    /**
     * @return Список объектов, которые были загружены.
     */
    @NotNull
    Collection<O> getObjects();

    /**
     * @return Режим продолжения обработки других Топиков.
     */
    @NotNull
    IncomeTopicsLoaderContinueMode getContinueMode();

    /**
     * Установка начальных значений перед "бросанием" события.
     * @param source                Источник события.
     * @param loadingDescriptor     Описатель загрузки из Топика.
     * @param objects               Список объектов, которые были загружены.
     * @return                      this.
     */
    @SuppressWarnings("UnusedReturnValue")
    @NotNull
    OnObjectsLoadedFromIncomeTopicEvent<O, P> reset(@NotNull final Object source, @NotNull final IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, @NotNull final Collection<O> objects);
}
