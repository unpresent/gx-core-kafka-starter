package ru.gxfin.common.kafka.events;

import org.jetbrains.annotations.NotNull;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.IncomeTopicsLoaderContinueMode;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

import java.util.Collection;

/**
 * Интерфейс объектов-событий, которые бросаются в процессе загрузки объектов.
 * Если в загрузке участвует репозиторий, то перед сохранением в репозиторий.
 * @param <O>   Класс загружаемых объектов.
 * @param <P>   Класс пакетов загружаемых объектов.
 */
@SuppressWarnings("unused")
public interface OnObjectsLoadingFromIncomeTopicEvent<O extends DataObject, P extends DataPackage<O>> {

    /**
     * @return Получение описателя загрузки из Топика.
     */
    IncomeTopicLoadingDescriptor<O, P> getLoadingDescriptor();

    /**
     * @return Список объектов, которые были загружены.
     */
    Collection<NewOldDataObjectsPair<O>> getChanges();

    /**
     * @return Режим продолжения обработки других Топиков.
     */
    IncomeTopicsLoaderContinueMode getContinueMode();

    /**
     * Установка начальных значений перед "бросанием" события.
     * @param source                Источник события.
     * @param loadingDescriptor     Описатель загрузки из Топика.
     * @param changes               Список изменений.
     * @return                      this.
     */
    @SuppressWarnings("UnusedReturnValue")
    OnObjectsLoadingFromIncomeTopicEvent<O, P> reset(Object source, @NotNull IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, @NotNull Collection<NewOldDataObjectsPair<O>> changes);
}
