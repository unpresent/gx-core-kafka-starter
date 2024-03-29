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
    private final StringHeader serviceHeaderClassName = new StringHeader(ServiceHeadersKeys.DATA_OBJECT_CLASS_NAME, null);

    @NotNull
    private final LongHeader serviceHeaderPackageSize = new LongHeader(ServiceHeadersKeys.DATA_PACKAGE_SIZE, 0);

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
     *
     * @param descriptor описатель исходящего канала.
     * @param message    выгружаемое сообщение.
     * @param headers    заголовки.
     */
    public <M extends Message<? extends MessageBody>>
    void uploadMessage(
            @NotNull final String workerName,
            @NotNull final KafkaOutcomeTopicUploadingDescriptor descriptor,
            @NotNull final M message,
            @Nullable final Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        this.serviceHeaders.clear();
        this.serviceHeaders.add(this.serviceHeaderClassName.setValue(message.getClass().getSimpleName()));
        internalUploadPreparedData(workerName, descriptor, message, headers, this.serviceHeaders);
    }

    /**
     * Выгрузка объекта (с упаковкой в сообщение, класс и конструктор которого определяется в описателе).
     *
     * @param descriptor описатель исходящего канала.
     * @param dataObject выгружаемый объект данных.
     * @param headers    заголовки.
     */
    public <M extends Message<? extends MessageBody>, D extends DataObject>
    void uploadObject(
            @NotNull final String workerName,
            @NotNull final KafkaOutcomeTopicUploadingDescriptor descriptor,
            @NotNull final D dataObject,
            @Nullable final Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        internalUploadDataObject(workerName, descriptor, dataObject, headers);
    }

    /**
     * Выгрузка списка объектов (с упаковкой в сообщения, класс и конструктор которого определяется в описателе).
     *
     * @param descriptor  описатель исходящего канала.
     * @param dataObjects выгружаемый объект данных.
     * @param headers     заголовки.
     */
    public <M extends Message<? extends MessageBody>, D extends DataObject>
    void uploadObjects(
            @NotNull final String workerName,
            @NotNull final KafkaOutcomeTopicUploadingDescriptor descriptor,
            @NotNull final Iterable<D> dataObjects,
            @Nullable final Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        for (final var dataObject : dataObjects) {
            internalUploadDataObject(workerName, descriptor, dataObject, headers);
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
    protected void checkDescriptorIsActive(@NotNull final KafkaOutcomeTopicUploadingDescriptor descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getChannelName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getChannelName() + " is not enabled!");
        }
    }

    @SuppressWarnings("unchecked")
    protected <M extends Message<? extends MessageBody>, D extends DataObject>
    void internalUploadDataObject(
            @NotNull final String workerName,
            @NotNull final KafkaOutcomeTopicUploadingDescriptor descriptor,
            @NotNull final D dataObject,
            @Nullable final Iterable<Header> headers
    ) throws Exception {
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }
        final var message = (M) this.messagesFactory
                .createByDataObject(
                        null,
                        api.getMessageType(),
                        api.getVersion(),
                        dataObject,
                        null
                );

        this.serviceHeaders.clear();
        this.serviceHeaders.add(this.serviceHeaderClassName.setValue(message.getClass().getSimpleName()));
        internalUploadPreparedData(workerName, descriptor, message, headers, this.serviceHeaders);
    }

    @SuppressWarnings("unchecked")
    protected <M extends Message<? extends MessageBody>>
    void internalUploadPreparedData(
            @NotNull final String workerName,
            @NotNull final KafkaOutcomeTopicUploadingDescriptor descriptor,
            @NotNull final M message,
            @Nullable final Iterable<Header> headers,
            @NotNull final Collection<Header> theServiceHeaders
    ) throws Exception {
        final var started = System.currentTimeMillis();
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        // Объединяем списки заголовков.
        // В подавляющем большинстве случаев будет 0 или 1 заголовок.
        final var allHeadersMap =
                descriptor.metadataSize() > 0 || headers != null || theServiceHeaders.size() > 0
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
        final var allHeaders =
                allHeadersMap != null && allHeadersMap.size() > 0
                        ? allHeadersMap.values()
                        : null;

        // RecordMetadata recordMetadata;
        if (api.getSerializeMode() == SerializeMode.JsonString) {
            final var serializedMessage = this.objectMapper.writeValueAsString(message);
            final var record = new ProducerRecord<Long, String>(
                    descriptor.getChannelName(),
                    null,
                    null,
                    serializedMessage,
                    allHeaders
            );
            final var producer = (Producer<Long, String>) descriptor.getProducer();
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (producer) {
                // Собственно отправка в Kafka:
                // recordMetadata = producer.send(record).get();
                producer.send(record);
            }
        } else {
            final var serializedMessage = this.objectMapper.writeValueAsBytes(message);
            final var record = new ProducerRecord<Long, byte[]>(
                    descriptor.getChannelName(),
                    null,
                    null,
                    serializedMessage,
                    allHeaders
            );
            final var producer = (Producer<Long, byte[]>) descriptor.getProducer();
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (producer) {
                // Собственно отправка в Kafka:
                // recordMetadata = producer.send(record).get();
                producer.send(record);
            }
        }

        descriptor.recordMessageExecuted(workerName, System.currentTimeMillis() - started);
        // return new PartitionOffset(recordMetadata.partition(), recordMetadata.offset());
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
