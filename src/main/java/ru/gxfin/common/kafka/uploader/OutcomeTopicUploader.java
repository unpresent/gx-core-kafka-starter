package ru.gxfin.common.kafka.uploader;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;

import java.util.Collection;

@SuppressWarnings("unused")
public interface OutcomeTopicUploader {
    <O extends DataObject, P extends DataPackage<O>> void uploadDataObject(OutcomeTopicUploadingDescriptor<O, P> descriptor, O object) throws Exception;

    <O extends DataObject, P extends DataPackage<O>> void uploadDataObjects(OutcomeTopicUploadingDescriptor<O, P> descriptor, Collection<O> objects) throws Exception;

    <O extends DataObject, P extends DataPackage<O>> void uploadDataPackage(OutcomeTopicUploadingDescriptor<O, P> descriptor, P dataPackage) throws Exception;
}
