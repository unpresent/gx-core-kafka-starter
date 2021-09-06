package ru.gxfin.common.kafka.unloader;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;

import java.util.Collection;

@SuppressWarnings("unused")
public interface OutcomeTopicUnloader {
    <O extends DataObject, P extends DataPackage<O>> void unloadDataObject(OutcomeTopicUnloadingDescriptor<O, P> descriptor, O object) throws Exception;

    <O extends DataObject, P extends DataPackage<O>> void unloadDataObjects(OutcomeTopicUnloadingDescriptor<O, P> descriptor, Collection<O> objects) throws Exception;

    <O extends DataObject, P extends DataPackage<O>> void unloadDataPackage(OutcomeTopicUnloadingDescriptor<O, P> descriptor, P dataPackage) throws Exception;
}
