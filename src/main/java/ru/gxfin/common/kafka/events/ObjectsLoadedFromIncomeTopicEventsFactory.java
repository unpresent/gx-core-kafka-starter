package ru.gxfin.common.kafka.events;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

/**
 * Интерфейс фабрики, предоставляющей объекты-события для вызова обработчиков событий о факте загрузки объектов.
 */
public interface ObjectsLoadedFromIncomeTopicEventsFactory {
    @SuppressWarnings("rawtypes")
    ObjectsLoadedFromIncomeTopicEvent getOrCreateEvent(Class<? extends ObjectsLoadedFromIncomeTopicEvent> eventClass, Object source, IncomeTopicLoadingDescriptor loadingDescriptor, Iterable<DataObject> objects);
}
