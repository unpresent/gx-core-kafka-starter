package ru.gx.kafka.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.kafka.LongHeader;
import ru.gx.kafka.ServiceHeadersKeys;
import ru.gx.kafka.StringHeader;
import ru.gx.kafka.offsets.PartitionOffset;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.TopicMessageMode;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

public class StandardOutcomeTopicsUploader implements OutcomeTopicUploader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    @NotNull
    private ObjectMapper objectMapper;

    @NotNull
    private final Map<OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>, PartitionOffset>
            lastPublishedSnapshots = new HashMap<>();

    @NotNull
    private final ArrayList<Header> serviceHeaders = new ArrayList<>();

    @NotNull
    private final StringHeader serviceHeaderClassName = new StringHeader(ServiceHeadersKeys.dataObjectClassName, null);

    @NotNull
    private final LongHeader serviceHeaderPackageSize = new LongHeader(ServiceHeadersKeys.dataPackageSize, 0);

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public StandardOutcomeTopicsUploader() {
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicUploader">

    /**
     * @param object  выгружаемый объект.
     * @param headers заголовки.
     * @param <O>     тип объекта.
     * @param <P>     тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился объект.
     */
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataObject(
            @NotNull final OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull final O object,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsInitialized(descriptor);

        this.serviceHeaders.clear();
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().add(object);
            this.serviceHeaders.add(this.serviceHeaderPackageSize.setValue(dataPackage.size()));
            return internalUploadData(descriptor, dataPackage, headers, this.serviceHeaders);
        } else {
            this.serviceHeaders.add(this.serviceHeaderClassName.setValue(object.getClass().getSimpleName()));
            return internalUploadData(descriptor, object, headers, this.serviceHeaders);
        }
    }

    /**
     * Выгрузить несколько объектов данных.
     *
     * @param descriptor описатель исходящей очереди.
     * @param objects    коллекция объектов.
     * @param headers    заголовки.
     * @param <O>        тип объекта.
     * @param <P>        тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataObjects(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull Iterable<O> objects,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsInitialized(descriptor);

        this.serviceHeaders.clear();
        PartitionOffset result = null;
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            this.serviceHeaders.add(this.serviceHeaderPackageSize);
            final var dataPackage = createPackage(descriptor);
            var i = 0;
            for (O object : objects) {
                if (i >= descriptor.getMaxPackageSize()) {
                    this.serviceHeaderPackageSize.setValue(dataPackage.size());
                    final var rs = internalUploadData(descriptor, dataPackage, headers, this.serviceHeaders);
                    result = result == null ? rs : result;
                    dataPackage.getObjects().clear();
                    i = 0;
                }
                dataPackage.getObjects().add(object);
                i++;
            }
            this.serviceHeaderPackageSize.setValue(dataPackage.size());
            return internalUploadData(descriptor, dataPackage, headers, this.serviceHeaders);
        } else {
            this.serviceHeaders.add(this.serviceHeaderClassName);
            for (var o : objects) {
                this.serviceHeaderClassName.setValue(o.getClass().getSimpleName());
                final var rs = internalUploadData(descriptor, o, headers, this.serviceHeaders);
                result = result == null ? rs : result;
            }
            return result != null ? result : new PartitionOffset(0, 0);
        }
    }

    /**
     * Выгрузить пакет объектов данных.
     *
     * @param descriptor  описатель исходящей очереди.
     * @param dataPackage пакет объектов.
     * @param headers     заголовки.
     * @param <O>         тип объекта.
     * @param <P>         тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataPackage(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull P dataPackage,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsInitialized(descriptor);

        this.serviceHeaders.clear();
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            this.serviceHeaders.add(this.serviceHeaderPackageSize.setValue(dataPackage.size()));
            return internalUploadData(descriptor, dataPackage, headers, this.serviceHeaders);
        } else {
            this.serviceHeaders.add(this.serviceHeaderClassName);
            PartitionOffset result = null;
            for (var o : dataPackage.getObjects()) {
                this.serviceHeaderClassName.setValue(o.getClass().getSimpleName());
                final var rs = internalUploadData(descriptor, o, headers, this.serviceHeaders);
                result = result == null ? rs : result;
            }
            return result != null ? result : new PartitionOffset(0, 0);
        }
    }

    /**
     * Выгрузить все объекты из MemoryRepository в данном описателе.
     *
     * @param descriptor описатель исходящей очереди.
     * @param headers    заголовки.
     * @param <O>        тип объекта.
     * @param <P>        тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset publishMemoryRepositorySnapshot(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @Nullable Iterable<Header> headers) throws Exception {
        final var memoryRepository = descriptor.getMemoryRepository();
        if (memoryRepository == null) {
            throw new OutcomeTopicsConfigurationException("The descriptor " + descriptor.getTopic() + " doesn't have MemoryRepository!");
        }
        return publishFullSnapshot(descriptor, memoryRepository.getAll(), headers);
    }


    /**
     * Выгрузить все объекты из MemoryRepository в данном описателе.
     * @param descriptor описатель исходящей очереди.
     * @param snapshotOffAllObjects полный snapshot - должен быть список всех объектов.
     * @param headers заголовки.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset publishFullSnapshot(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull Iterable<O> snapshotOffAllObjects,
            @Nullable Iterable<Header> headers) throws Exception {
        final var result = uploadDataObjects(descriptor, snapshotOffAllObjects, headers);
        this.lastPublishedSnapshots.put((OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>)descriptor, result);
        return result;
    }


    /**
     * Получение offset-а последней выгрузки полного snapshot-а данных из MemoryRepository.
     * @param descriptor описатель исходящей очереди.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект в последнем snapshot-е.
     */
    @Nullable
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset getLastPublishedSnapshotOffset(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor
    ) {
        return this.lastPublishedSnapshots.get(descriptor);
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя логика">

    /**
     * Проверка на то, был ли инициализирован описатель.
     *
     * @param descriptor описатель.
     * @param <O>        тип объекта.
     * @param <P>        тип пакета объектов.
     */
    protected <O extends DataObject, P extends DataPackage<O>>
    void checkDescriptorIsInitialized(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor) {
        if (!descriptor.isInitialized()) {
            throw new InvalidParameterException("Topic descriptor " + descriptor.getTopic() + " is not initialized!");
        }
    }

    @NotNull
    protected <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset internalUploadData(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull Object data,
            @Nullable Iterable<Header> headers,
            @NotNull Collection<Header> theServiceHeaders
    ) throws Exception {
        final var message = this.objectMapper.writeValueAsString(data);
        final var producer = descriptor.getProducer();

        // Объединяем списки заголовков.
        // В подавляющем большинстве случаев будет 0 или 1 заголовок.
        final var allHeadersMap = (descriptor.getDescriptorHeadersSize() > 0 || headers != null || theServiceHeaders.size() > 0)
                ? new HashMap<String, Header>()
                : null;
        if (allHeadersMap != null) {
            descriptor.getDescriptorHeaders().forEach(header -> allHeadersMap.put(header.key(), header));
            theServiceHeaders.forEach(header -> allHeadersMap.put(header.key(), header));
            if (headers != null) {
                headers.forEach(header -> allHeadersMap.put(header.key(), header));
            }
        }

        // Если 0 заголовков, то в конструктор ProducerRecord передаем null.
        final var allHeaders = allHeadersMap != null && allHeadersMap.size() > 0 ? allHeadersMap.values() : null;
        final var record = new ProducerRecord<Long, String>(descriptor.getTopic(), null, null, message, allHeaders);
        // Собственно отправка в Kafka:
        final var recordMetadata = producer.send(record).get();
        return new PartitionOffset(recordMetadata.partition(), recordMetadata.offset());
    }

    /**
     * Создание нового экземпляра пакета объектов.
     *
     * @param descriptor описатель.
     * @param <O>        тип объекта.
     * @param <P>        тип пакета объектов.
     * @return пакет объектов.
     */
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    P createPackage(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor) throws Exception {
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
