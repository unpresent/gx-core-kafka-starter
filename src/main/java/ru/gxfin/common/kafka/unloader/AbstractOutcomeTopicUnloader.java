package ru.gxfin.common.kafka.unloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.TopicMessageMode;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

public abstract class AbstractOutcomeTopicUnloader implements OutcomeTopicUnloader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Объект контекста требуется для вызова событий.
     */
    private final ApplicationContext context;

    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    private final ObjectMapper objectMapper;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractOutcomeTopicUnloader(ApplicationContext context, ObjectMapper objectMapper) {
        this.context = context;
        this.objectMapper = objectMapper;
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    @Override
    public <O extends DataObject, P extends DataPackage<O>> void unloadDataObject(OutcomeTopicUnloadingDescriptor<O, P> descriptor, O object) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().add(object);
            internalUnloadPackage(descriptor, dataPackage);
        } else {
            internalUnloadObject(descriptor, object);
        }
    }

    @Override
    public <O extends DataObject, P extends DataPackage<O>> void unloadDataObjects(OutcomeTopicUnloadingDescriptor<O, P> descriptor, Collection<O> objects) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().addAll(objects);
            internalUnloadPackage(descriptor, dataPackage);
        } else {
            for (var o : objects) {
                internalUnloadObject(descriptor, o);
            }
        }
    }

    @Override
    public <O extends DataObject, P extends DataPackage<O>> void unloadDataPackage(OutcomeTopicUnloadingDescriptor<O, P> descriptor, P dataPackage) throws Exception {
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            internalUnloadPackage(descriptor, dataPackage);
        } else {
            for (var o : dataPackage.getObjects()) {
                internalUnloadObject(descriptor, o);
            }
        }
    }

    protected <O extends DataObject, P extends DataPackage<O>> void internalUnloadPackage(OutcomeTopicUnloadingDescriptor<O, P> descriptor, P dataPackage) throws JsonProcessingException, ExecutionException, InterruptedException {
        final var message = this.objectMapper.writeValueAsString(dataPackage);
        final var producer = descriptor.getProducer();
        producer.send(new ProducerRecord<>(descriptor.getTopic(), message)).get();
    }

    protected <O extends DataObject, P extends DataPackage<O>> void internalUnloadObject(OutcomeTopicUnloadingDescriptor<O, P> descriptor, O dataObject) throws JsonProcessingException, ExecutionException, InterruptedException {
        final var message = this.objectMapper.writeValueAsString(dataObject);
        final var producer = descriptor.getProducer();
        producer.send(new ProducerRecord<>(descriptor.getTopic(), message)).get();
    }

    protected <O extends DataObject, P extends DataPackage<O>> P createPackage(OutcomeTopicUnloadingDescriptor<O, P> descriptor) throws Exception {
        final var packageClass = descriptor.getDataPackageClass();
        if (packageClass != null) {
            final var constructor = packageClass.getConstructor();
            return constructor.newInstance();
        } else {
            throw new Exception("Can't create DataPackage!");
        }
    }
}
