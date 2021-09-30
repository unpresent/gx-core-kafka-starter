package ru.gx.kafka.upload;

import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.PartitionOffset;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

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
