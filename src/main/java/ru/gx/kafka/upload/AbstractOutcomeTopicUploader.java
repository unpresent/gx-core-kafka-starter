package ru.gx.kafka.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.kafka.PartitionOffset;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.TopicMessageMode;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

public abstract class AbstractOutcomeTopicUploader implements OutcomeTopicsConfiguration, OutcomeTopicUploader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    @NotNull
    private ObjectMapper objectMapper;

    /**
     * Список описателей с группировкой по топикам.
     */
    @NotNull
    private final Map<String, OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> topics = new HashMap<>();

    @NotNull
    private final Map<OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>, PartitionOffset>
            lastPublishedSnapshots = new HashMap<>();

    @Getter
    @NotNull
    private final OutcomeTopicUploadingDescriptorsDefaults descriptorsDefaults = new OutcomeTopicUploadingDescriptorsDefaults();

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractOutcomeTopicUploader() {
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicsConfiguration">
    @Override
    public boolean contains(@NotNull String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    @SuppressWarnings("unchecked")
    @Override
    public @NotNull <O extends DataObject, P extends DataPackage<O>>
    OutcomeTopicUploadingDescriptor<O, P> get(@NotNull String topic) {
        final var result = (OutcomeTopicUploadingDescriptor<O, P>) this.topics.get(topic);
        if (result == null) {
            throw new OutcomeTopicsConfigurationException("Can't find description for topic " + topic);
        }
        return result;
    }

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    @Override
    public @NotNull <O extends DataObject, P extends DataPackage<O>>
    OutcomeTopicsConfiguration register(@NotNull OutcomeTopicUploadingDescriptor<O, P> item) throws InvalidParameterException {
        if (contains(item.getTopic())) {
            throw new OutcomeTopicsConfigurationException("Topic " + item.getTopic() + " already registered!");
        }

        if (!item.isInitialized()) {
            final var props = getDescriptorsDefaults().getProducerProperties();
            if (props == null) {
                throw new OutcomeTopicsConfigurationException("Invalid null value getDescriptorsDefaults().getProducerProperties()!");
            }
            item.init(props);
        }

        final var localItem = (OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>) item;
        topics.put(item.getTopic(), localItem);

        return this;
    }

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
    @Override
    public @NotNull OutcomeTopicsConfiguration unregister(@NotNull String topic) {
        if (!contains(topic)) {
            throw new OutcomeTopicsConfigurationException("Topic " + topic + " not registered!");
        }

        this.topics.remove(topic);
        return this;
    }

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @Override
    public @NotNull Iterable<OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getAll() {
        return this.topics.values();
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

        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().add(object);
            return internalUploadData(descriptor, dataPackage, headers);
        } else {
            return internalUploadData(descriptor, object, headers);
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

        PartitionOffset result = null;
        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            final var dataPackage = createPackage(descriptor);
            var i = 0;
            for (O object : objects) {
                if (i >= descriptor.getMaxPackageSize()) {
                    final var rs = internalUploadData(descriptor, dataPackage, headers);
                    result = result == null ? rs : result;
                    dataPackage.getObjects().clear();
                    i = 0;
                }
                dataPackage.getObjects().add(object);
                i++;
            }
            return internalUploadData(descriptor, dataPackage, headers);
        } else {
            for (var o : objects) {
                final var rs = internalUploadData(descriptor, o, headers);
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

        if (descriptor.getMessageMode() == TopicMessageMode.PACKAGE) {
            return internalUploadData(descriptor, dataPackage, headers);
        } else {
            PartitionOffset result = null;
            for (var o : dataPackage.getObjects()) {
                final var rs = internalUploadData(descriptor, o, headers);
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
            @Nullable Iterable<Header> headers
    ) throws Exception {
        final var message = this.objectMapper.writeValueAsString(data);
        final var producer = descriptor.getProducer();

        // Объединяем списки заголовков.
        // В подавляющем большинстве случаев будет 0 или 1 заголовок.
        final var allHeadersMap = (descriptor.getDescriptorHeadersSize() > 0 || headers != null)
                ? new HashMap<String, Header>()
                : null;
        if (allHeadersMap != null) {
            descriptor.getDescriptorHeaders().forEach(header -> allHeadersMap.put(header.key(), header));
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
    protected <O extends DataObject, P extends DataPackage<O>>
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
