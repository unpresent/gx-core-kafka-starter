package ru.gx.core.kafka.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.ChannelMessageMode;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.data.DataObject;
import ru.gx.core.data.DataPackage;
import ru.gx.core.kafka.LongHeader;
import ru.gx.core.kafka.ServiceHeadersKeys;
import ru.gx.core.kafka.StringHeader;
import ru.gx.core.kafka.offsets.PartitionOffset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import static lombok.AccessLevel.PROTECTED;

@SuppressWarnings("unused")
public class KafkaOutcomeTopicsUploader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    @NotNull
    private ObjectMapper objectMapper;

    @NotNull
    private final ArrayList<Header> serviceHeaders = new ArrayList<>();

    @NotNull
    private final StringHeader serviceHeaderClassName = new StringHeader(ServiceHeadersKeys.dataObjectClassName, null);

    @NotNull
    private final LongHeader serviceHeaderPackageSize = new LongHeader(ServiceHeadersKeys.dataPackageSize, 0);

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public KafkaOutcomeTopicsUploader() {
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicUploader">

    /**
     * @param object  выгружаемый объект.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился объект.
     */
    @NotNull
    public
    <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataObject(
            @NotNull final KafkaOutcomeTopicLoadingDescriptor<O, P> descriptor,
            @NotNull final O object,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        this.serviceHeaders.clear();
        if (descriptor.getMessageMode() == ChannelMessageMode.Package) {
            final var dataPackage = createPackage(descriptor);
            dataPackage.getObjects().add(object);
            this.serviceHeaders.add(this.serviceHeaderPackageSize.setValue(dataPackage.size()));
            return internalUploadPreparedData(descriptor, dataPackage, headers, this.serviceHeaders);
        } else {
            this.serviceHeaders.add(this.serviceHeaderClassName.setValue(object.getClass().getSimpleName()));
            return internalUploadPreparedData(descriptor, object, headers, this.serviceHeaders);
        }
    }

    /**
     * Выгрузить несколько объектов данных.
     *
     * @param descriptor описатель исходящей очереди.
     * @param objects    коллекция объектов.
     * @param headers    заголовки.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataObjects(
            @NotNull KafkaOutcomeTopicLoadingDescriptor<O, P> descriptor,
            @NotNull Iterable<O> objects,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        this.serviceHeaders.clear();
        PartitionOffset result = null;
        if (descriptor.getMessageMode() == ChannelMessageMode.Package) {
            this.serviceHeaders.add(this.serviceHeaderPackageSize);
            final var dataPackage = createPackage(descriptor);
            var i = 0;
            for (var object : objects) {
                if (i >= descriptor.getMaxPackageSize()) {
                    this.serviceHeaderPackageSize.setValue(dataPackage.size());
                    final var rs = internalUploadPreparedData(descriptor, dataPackage, headers, this.serviceHeaders);
                    result = result == null ? rs : result;
                    dataPackage.getObjects().clear();
                    i = 0;
                }
                dataPackage.getObjects().add(object);
                i++;
            }
            this.serviceHeaderPackageSize.setValue(dataPackage.size());
            return internalUploadPreparedData(descriptor, dataPackage, headers, this.serviceHeaders);
        } else {
            this.serviceHeaders.add(this.serviceHeaderClassName);
            for (var o : objects) {
                this.serviceHeaderClassName.setValue(o.getClass().getSimpleName());
                final var rs = internalUploadPreparedData(descriptor, o, headers, this.serviceHeaders);
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
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataPackage(
            @NotNull KafkaOutcomeTopicLoadingDescriptor<O, P> descriptor,
            @NotNull P dataPackage,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        this.serviceHeaders.clear();
        if (descriptor.getMessageMode() == ChannelMessageMode.Package) {
            this.serviceHeaders.add(this.serviceHeaderPackageSize.setValue(dataPackage.size()));
            return internalUploadPreparedData(descriptor, dataPackage, headers, this.serviceHeaders);
        } else {
            this.serviceHeaders.add(this.serviceHeaderClassName);
            PartitionOffset result = null;
            for (var o : dataPackage.getObjects()) {
                this.serviceHeaderClassName.setValue(o.getClass().getSimpleName());
                final var rs = internalUploadPreparedData(descriptor, o, headers, this.serviceHeaders);
                result = result == null ? rs : result;
            }
            return result != null ? result : new PartitionOffset(0, 0);
        }
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя логика">
    /**
     * Проверка на то, был ли инициализирован описатель.
     *
     * @param descriptor описатель.
     */
    protected void checkDescriptorIsActive(@NotNull final KafkaOutcomeTopicLoadingDescriptor<?, ?> descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getName() + " is not enabled!");
        }
    }

    @SuppressWarnings("unchecked")
    @NotNull
    protected <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset internalUploadPreparedData(
            @NotNull KafkaOutcomeTopicLoadingDescriptor<O, P> descriptor,
            @NotNull Object data,
            @Nullable Iterable<Header> headers,
            @NotNull Collection<Header> theServiceHeaders
    ) throws Exception {

        // Объединяем списки заголовков.
        // В подавляющем большинстве случаев будет 0 или 1 заголовок.
        final var allHeadersMap = (descriptor.metadataSize() > 0 || headers != null || theServiceHeaders.size() > 0)
                ? new HashMap<String, Header>()
                : null;
        if (allHeadersMap != null) {
            descriptor.getAllMetadata()
                    .forEach(md -> {
                        if (md.getValue() != null) {
                            final var header = new StringHeader(md.getKey().toString(), md.getValue().toString());
                            allHeadersMap.put(header.key(), header);
                        }
                    });
            theServiceHeaders.forEach(header -> allHeadersMap.put(header.key(), header));
            if (headers != null) {
                headers.forEach(header -> allHeadersMap.put(header.key(), header));
            }
        }

        // Если 0 заголовков, то в конструктор ProducerRecord передаем null.
        final var allHeaders = allHeadersMap != null && allHeadersMap.size() > 0 ? allHeadersMap.values() : null;

        RecordMetadata recordMetadata;
        if (descriptor.getSerializeMode() == SerializeMode.JsonString) {
            final var producer = (Producer<Long, String>) descriptor.getProducer();
            final var message = this.objectMapper.writeValueAsString(data);
            final var record = new ProducerRecord<Long, String>(descriptor.getName(), null, null, message, allHeaders);
            // Собственно отправка в Kafka:
            recordMetadata = producer.send(record).get();
        } else {
            final var producer = (Producer<Long, byte[]>) descriptor.getProducer();
            final var message = this.objectMapper.writeValueAsBytes(data);
            final var record = new ProducerRecord<Long, byte[]>(descriptor.getName(), null, null, message, allHeaders);
            // Собственно отправка в Kafka:
            recordMetadata = producer.send(record).get();
        }
        return new PartitionOffset(recordMetadata.partition(), recordMetadata.offset());
    }

    /**
     * Создание нового экземпляра пакета объектов.
     *
     * @param descriptor описатель.
     * @return пакет объектов.
     */
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    P createPackage(@NotNull final KafkaOutcomeTopicLoadingDescriptor<O, P> descriptor) throws Exception {
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
