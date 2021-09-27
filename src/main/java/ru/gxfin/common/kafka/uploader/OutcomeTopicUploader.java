package ru.gxfin.common.kafka.uploader;

import org.jetbrains.annotations.NotNull;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.loader.PartitionOffset;

import java.util.Collection;

@SuppressWarnings("unused")
public interface OutcomeTopicUploader {
    @NotNull
    <O extends DataObject, P extends DataPackage<O>> PartitionOffset uploadDataObject(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull O object) throws Exception;

    @NotNull
    <O extends DataObject, P extends DataPackage<O>> PartitionOffset uploadDataObjects(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull Collection<O> objects) throws Exception;

    @NotNull
    <O extends DataObject, P extends DataPackage<O>> PartitionOffset uploadDataPackage(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull P dataPackage) throws Exception;
}
