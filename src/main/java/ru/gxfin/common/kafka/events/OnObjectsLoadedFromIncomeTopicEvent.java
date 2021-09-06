package ru.gxfin.common.kafka.events;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.IncomeTopicsLoaderContinueMode;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

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
    IncomeTopicLoadingDescriptor<O, P> getLoadingDescriptor();

    /**
     * @return Список объектов, которые были загружены.
     */
    Collection<O> getObjects();

    /**
     * @return Режим продолжения обработки других Топиков.
     */
    IncomeTopicsLoaderContinueMode getContinueMode();

    /**
     * Установка начальных значений перед "бросанием" события.
     * @param source                Источник события.
     * @param loadingDescriptor     Описатель загрузки из Топика.
     * @param objects               Список объектов, которые были загружены.
     * @return                      this.
     */
    @SuppressWarnings("UnusedReturnValue")
    OnObjectsLoadedFromIncomeTopicEvent<O, P> reset(Object source, IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, Collection<O> objects);
}
