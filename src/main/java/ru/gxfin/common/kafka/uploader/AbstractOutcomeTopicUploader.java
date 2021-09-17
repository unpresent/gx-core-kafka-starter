package ru.gxfin.common.kafka.uploader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.loader.PartitionOffset;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

public abstract class AbstractOutcomeTopicUploader implements OutcomeTopicUploader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    private final ObjectMapper objectMapper;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractOutcomeTopicUploader(@NotNull ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicUploader">
    @Override
    public <O extends DataObject, P extends DataPackage<O>> PartitionOffset uploadDataObject(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull O object) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().add(object);
            return internalUploadPackage(descriptor, dataPackage);
        } else {
            return internalUploadObject(descriptor, object);
        }
    }

    @Override
    public <O extends DataObject, P extends DataPackage<O>> PartitionOffset uploadDataObjects(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull Collection<O> objects) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().addAll(objects);
            return internalUploadPackage(descriptor, dataPackage);
        } else {
            PartitionOffset result = null;
            for (var o : objects) {
                final var rs = internalUploadObject(descriptor, o);
                result = result == null ? rs : result;
            }
            return result;
        }
    }

    @Override
    public <O extends DataObject, P extends DataPackage<O>> PartitionOffset uploadDataPackage(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull P dataPackage) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            return internalUploadPackage(descriptor, dataPackage);
        } else {
            PartitionOffset result = null;
            for (var o : dataPackage.getObjects()) {
                final var rs = internalUploadObject(descriptor, o);
                result = result == null ? rs : result;
            }
            return result;
        }
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя логика">
    protected <O extends DataObject, P extends DataPackage<O>> PartitionOffset internalUploadPackage(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull P dataPackage) throws JsonProcessingException, ExecutionException, InterruptedException {
        final var message = this.objectMapper.writeValueAsString(dataPackage);
        final var producer = descriptor.getProducer();
        final var recordMetadata = producer.send(new ProducerRecord<>(descriptor.getTopic(), message)).get();
        return new PartitionOffset(recordMetadata.partition(), recordMetadata.offset());
    }

    protected <O extends DataObject, P extends DataPackage<O>> PartitionOffset internalUploadObject(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor, @NotNull O dataObject) throws JsonProcessingException, ExecutionException, InterruptedException {
        final var message = this.objectMapper.writeValueAsString(dataObject);
        final var producer = descriptor.getProducer();
        final var recordMetadata = producer.send(new ProducerRecord<>(descriptor.getTopic(), message)).get();
        return new PartitionOffset(recordMetadata.partition(), recordMetadata.offset());
    }

    protected <O extends DataObject, P extends DataPackage<O>> P createPackage(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor) throws Exception {
        final var packageClass = descriptor.getDataPackageClass();
        if (packageClass != null) {
            final var constructor = packageClass.getConstructor();
            return constructor.newInstance();
        } else {
            throw new Exception("Can't create DataPackage!");
        }
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
