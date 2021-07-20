package ru.gxfin.common.kafka.events;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

/**
 * Интерфейс фабрики, предоставляющей объекты-события для вызова обработчиков событий о факте загрузки объектов.
 * @param <O> Тип DataObject-ов, который были загружены.
 */
public interface ObjectsLoadedFromIncomeTopicEventsFactory {
    @SuppressWarnings("rawtypes")
    AbstractObjectsLoadedFromIncomeTopicEvent getOrCreateEvent(Class<ObjectsLoadedFromIncomeTopicEvent> eventClass, Object source, IncomeTopicLoadingDescriptor loadingDescriptor, Iterable<DataObject> objects);

    enum GettingMode {
        /**
         * Всегда создавать новый объект-событие
         */
        New,

        /**
         * Выдавать всегда одну и ту же заготовку
         */
        Singleton
    }
}
