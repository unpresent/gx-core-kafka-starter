package ru.gxfin.common.kafka.uploader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.TopicMessageMode;

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
    protected AbstractOutcomeTopicUploader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    @Override
    public <O extends DataObject, P extends DataPackage<O>> void uploadDataObject(OutcomeTopicUploadingDescriptor<O, P> descriptor, O object) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().add(object);
            internalUploadPackage(descriptor, dataPackage);
        } else {
            internalUploadObject(descriptor, object);
        }
    }

    @Override
    public <O extends DataObject, P extends DataPackage<O>> void uploadDataObjects(OutcomeTopicUploadingDescriptor<O, P> descriptor, Collection<O> objects) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().addAll(objects);
            internalUploadPackage(descriptor, dataPackage);
        } else {
            for (var o : objects) {
                internalUploadObject(descriptor, o);
            }
        }
    }

    @Override
    public <O extends DataObject, P extends DataPackage<O>> void uploadDataPackage(OutcomeTopicUploadingDescriptor<O, P> descriptor, P dataPackage) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            internalUploadPackage(descriptor, dataPackage);
        } else {
            for (var o : dataPackage.getObjects()) {
                internalUploadObject(descriptor, o);
            }
        }
    }

    protected <O extends DataObject, P extends DataPackage<O>> void internalUploadPackage(OutcomeTopicUploadingDescriptor<O, P> descriptor, P dataPackage) throws JsonProcessingException, ExecutionException, InterruptedException {
        final var message = this.objectMapper.writeValueAsString(dataPackage);
        final var producer = descriptor.getProducer();
        producer.send(new ProducerRecord<>(descriptor.getTopic(), message)).get();
    }

    protected <O extends DataObject, P extends DataPackage<O>> void internalUploadObject(OutcomeTopicUploadingDescriptor<O, P> descriptor, O dataObject) throws JsonProcessingException, ExecutionException, InterruptedException {
        final var message = this.objectMapper.writeValueAsString(dataObject);
        final var producer = descriptor.getProducer();
        producer.send(new ProducerRecord<>(descriptor.getTopic(), message)).get();
    }

    protected <O extends DataObject, P extends DataPackage<O>> P createPackage(OutcomeTopicUploadingDescriptor<O, P> descriptor) throws Exception {
        final var packageClass = descriptor.getDataPackageClass();
        if (packageClass != null) {
            final var constructor = packageClass.getConstructor();
            return constructor.newInstance();
        } else {
            throw new Exception("Can't create DataPackage!");
        }
    }
}
