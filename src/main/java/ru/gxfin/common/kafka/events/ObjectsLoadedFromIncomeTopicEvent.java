package ru.gxfin.common.kafka.events;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.IncomeTopicsLoaderContinueMode;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

@SuppressWarnings("unused")
public interface ObjectsLoadedFromIncomeTopicEvent<O extends DataObject> {
    @SuppressWarnings("rawtypes")
    IncomeTopicLoadingDescriptor getLoadingDescriptor();

    Iterable<O> getObjects();

    IncomeTopicsLoaderContinueMode getContinueMode();

    @SuppressWarnings("UnusedReturnValue")
    ObjectsLoadedFromIncomeTopicEvent<O> reset(Object source, IncomeTopicLoadingDescriptor<O> loadingDescriptor, Iterable<O> objects);
}
