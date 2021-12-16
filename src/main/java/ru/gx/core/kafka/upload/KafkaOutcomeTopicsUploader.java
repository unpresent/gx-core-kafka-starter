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
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.kafka.LongHeader;
import ru.gx.core.kafka.ServiceHeadersKeys;
import ru.gx.core.kafka.StringHeader;
import ru.gx.core.kafka.offsets.PartitionOffset;
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
     * @param message выгружаемое сообщение.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился объект.
     */
    @NotNull
    public <M extends Message<? extends MessageHeader, ? extends MessageBody>> PartitionOffset uploadMessage(
            @NotNull final KafkaOutcomeTopicLoadingDescriptor<M> descriptor,
            @NotNull final M message,
            @Nullable Iterable<Header> headers
    ) throws Exception {
        checkDescriptorIsActive(descriptor);

        this.serviceHeaders.clear();
        this.serviceHeaders.add(this.serviceHeaderClassName.setValue(message.getClass().getSimpleName()));
        return internalUploadPreparedData(descriptor, message, headers, this.serviceHeaders);
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя логика">
    /**
     * Проверка на то, был ли инициализирован описатель.
     *
     * @param descriptor описатель.
     */
    protected void checkDescriptorIsActive(@NotNull final KafkaOutcomeTopicLoadingDescriptor<?> descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getApi().getName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Topic descriptor " + descriptor.getApi().getName() + " is not enabled!");
        }
    }

    @SuppressWarnings("unchecked")
    @NotNull
    protected <M extends Message<? extends MessageHeader, ? extends MessageBody>>
    PartitionOffset internalUploadPreparedData(
            @NotNull KafkaOutcomeTopicLoadingDescriptor<M> descriptor,
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

        RecordMetadata recordMetadata;
        if (descriptor.getApi().getSerializeMode() == SerializeMode.JsonString) {
            final var producer = (Producer<Long, String>) descriptor.getProducer();
            final var serializedMessage = this.objectMapper.writeValueAsString(message);
            final var record = new ProducerRecord<Long, String>(descriptor.getApi().getName(), null, null, serializedMessage, allHeaders);
            // Собственно отправка в Kafka:
            recordMetadata = producer.send(record).get();
        } else {
            final var producer = (Producer<Long, byte[]>) descriptor.getProducer();
            final var serializedMessage = this.objectMapper.writeValueAsBytes(message);
            final var record = new ProducerRecord<Long, byte[]>(descriptor.getApi().getName(), null, null, serializedMessage, allHeaders);
            // Собственно отправка в Kafka:
            recordMetadata = producer.send(record).get();
        }
        return new PartitionOffset(recordMetadata.partition(), recordMetadata.offset());
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
