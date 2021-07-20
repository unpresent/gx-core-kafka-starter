package ru.gxfin.common.kafka.events;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.IncomeTopicsLoaderContinueMode;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

public interface ObjectsLoadedFromIncomeTopicEvent<O extends DataObject> {
    IncomeTopicLoadingDescriptor getLoadingDescriptor();

    Iterable<O> getObjects();

    IncomeTopicsLoaderContinueMode getContinueMode();
}
