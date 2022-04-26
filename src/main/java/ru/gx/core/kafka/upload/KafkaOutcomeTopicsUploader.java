package ru.gx.core.kafka.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.data.DataObject;
import ru.gx.core.kafka.LongHeader;
import ru.gx.core.kafka.ServiceHeadersKeys;
import ru.gx.core.kafka.StringHeader;
import ru.gx.core.messaging.DefaultMessagesFactory;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;

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
    @NotNull
    private final ObjectMapper objectMapper;

    @Getter(PROTECTED)
    @NotNull
    private final DefaultMessagesFactory messagesFactory;

    @NotNull
    private final ArrayList<Header> serviceHeaders = new ArrayList<>();

    @NotNull
    private final StringHeader serviceHeaderClassName = new StringHeader(ServiceHeadersKeys.dataObjectClassName, null);

    @NotNull
    private final LongHeader serviceHeaderPackageSize = new LongHeader(ServiceHeadersKeys.dataPackageSize, 0);

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public KafkaOutcomeTopicsUploader(@NotNull final ObjectMapper objectMapper, @NotNull final DefaultMessagesFactory messagesFactory) {
        this.objectMapper = objectMapper;
        this.messagesFactory = messagesFactory;
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicUploader">

    /**
     * Выгрузка сообщения.
     * @param descriptor описатель исходящего канала.
     * @param message выгружаемое сообщение.
     * @param headers заголовки.
     */
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>>
    void uploadMessage(
            @NotNull final KafkaOutcomeTopicUploadingDescriptor<M> descriptor,
            @NotNull final M message,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        this.serviceHeaders.clear();
        this.serviceHeaders.add(this.serviceHeaderClassName.setValue(message.getClass().getSimpleName()));
        internalUploadPreparedData(descriptor, message, headers, this.serviceHeaders);
    }

    /**
     * Выгрузка объекта (с упаковкой в сообщение, класс и конструктор которого определяется в описателе).
     * @param descriptor описатель исходящего канала.
     * @param dataObject выгружаемый объект данных.
     * @param headers заголовки.
     */
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>, D extends DataObject>
    void uploadObject(
            @NotNull final KafkaOutcomeTopicUploadingDescriptor<M> descriptor,
            @NotNull final D dataObject,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        internalUploadDataObject(descriptor, dataObject, headers);
    }

    /**
     * Выгрузка списка объектов (с упаковкой в сообщения, класс и конструктор которого определяется в описателе).
     * @param descriptor описатель исходящего канала.
     * @param dataObjects выгружаемый объект данных.
     * @param headers заголовки.
     */
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>, D extends DataObject>
    void uploadObjects(
            @NotNull final KafkaOutcomeTopicUploadingDescriptor<M> descriptor,
            @NotNull final Iterable<D> dataObjects,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        for (final var dataObject : dataObjects) {
            internalUploadDataObject(descriptor, dataObject, headers);
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
    protected void checkDescriptorIsActive(@NotNull final KafkaOutcomeTopicUploadingDescriptor<?> descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getApi().getName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getApi().getName() + " is not enabled!");
        }
    }

    @SuppressWarnings("unchecked")
    protected <M extends Message<? extends MessageHeader, ? extends MessageBody>, D extends DataObject>
    void internalUploadDataObject(
            @NotNull final KafkaOutcomeTopicUploadingDescriptor<M> descriptor,
            @NotNull final D dataObject,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        final var message = (M)this.messagesFactory
                .createByDataObject(null, descriptor.getApi().getMessageType(), descriptor.getApi().getVersion(), dataObject, null);

        this.serviceHeaders.clear();
        this.serviceHeaders.add(this.serviceHeaderClassName.setValue(message.getClass().getSimpleName()));
        internalUploadPreparedData(descriptor, message, headers, this.serviceHeaders);
    }

    @SuppressWarnings("unchecked")
    protected <M extends Message<? extends MessageHeader, ? extends MessageBody>>
    void internalUploadPreparedData(
            @NotNull KafkaOutcomeTopicUploadingDescriptor<M> descriptor,
            @NotNull M message,
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

        // RecordMetadata recordMetadata;
        if (descriptor.getApi().getSerializeMode() == SerializeMode.JsonString) {
            final var producer = (Producer<Long, String>) descriptor.getProducer();
            final var serializedMessage = this.objectMapper.writeValueAsString(message);
            final var record = new ProducerRecord<Long, String>(descriptor.getApi().getName(), null, null, serializedMessage, allHeaders);
            // Собственно отправка в Kafka:
            // recordMetadata = producer.send(record).get();
            producer.send(record);
        } else {
            final var producer = (Producer<Long, byte[]>) descriptor.getProducer();
            final var serializedMessage = this.objectMapper.writeValueAsBytes(message);
            final var record = new ProducerRecord<Long, byte[]>(descriptor.getApi().getName(), null, null, serializedMessage, allHeaders);
            // Собственно отправка в Kafka:
            // recordMetadata = producer.send(record).get();
            producer.send(record);
        }
        // return new PartitionOffset(recordMetadata.partition(), recordMetadata.offset());
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
