package ru.gx.kafka.upload;

import lombok.*;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.SerializeMode;
import ru.gx.kafka.TopicMessageMode;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class AbstractOutcomeTopicUploadingDescriptor implements OutcomeTopicUploadingDescriptor {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Конфигурация, которой принадлежит описатель.
     */
    @Getter(AccessLevel.PROTECTED)
    @NotNull
    private final AbstractOutcomeTopicsConfiguration owner;

    /**
     * Имя топика очереди.
     */
    @Getter
    @NotNull
    private final String topic;

    /**
     * Приоритет, с которым надо обрабатывать очередь.
     * 0 - высший.
     * > 0 - менее приоритетный.
     */
    @Getter
    private int priority;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Getter
    @NotNull
    private TopicMessageMode messageMode;

    @Getter
    @NotNull
    private SerializeMode serializeMode;

    /**
     * Ограничение по количеству Record-ов, отправляемых за раз.
     */
    @Getter
    @Setter
    private int maxBatchSize = 100;

    /**
     * Producer - публикатор сообщений в Kafka
     */
    @Getter
    private Producer<Long, ?> producer;

    @NotNull
    private final ArrayList<Header> descriptorHeaders = new ArrayList<>();

    /**
     * Признак того, что описатель инициализирован
     */
    @Getter
    private boolean initialized;
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">

    public AbstractOutcomeTopicUploadingDescriptor(@NotNull final AbstractOutcomeTopicsConfiguration owner, @NotNull final String topic, final OutcomeTopicUploadingDescriptorsDefaults defaults) {
        this.owner = owner;
        this.topic = topic;
        this.messageMode = TopicMessageMode.Object;
        this.serializeMode = SerializeMode.String;
        if (defaults != null) {
            this
                    .setMessageMode(defaults.getTopicMessageMode())
                    .setDescriptorHeaders(defaults.getDefaultHeaders())
                    .setMaxBatchSize(defaults.getMaxPackageSize());
        }
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @NotNull
    public AbstractOutcomeTopicUploadingDescriptor init(@NotNull final Properties producerProperties) throws InvalidParameterException {
        if (this.serializeMode == SerializeMode.String) {
            this.producer = new KafkaProducer<Long, String>(producerProperties);
        } else {
            this.producer = new KafkaProducer<Long, byte[]>(producerProperties);
        }
        this.initialized = true;
        this.owner.internalRegisterDescriptor(this);
        return this;
    }

    @Override
    public @NotNull OutcomeTopicUploadingDescriptor init() throws InvalidParameterException {
        return init(this.owner.getDescriptorsDefaults().getProducerProperties());
    }

    @NotNull
    public AbstractOutcomeTopicUploadingDescriptor unInit() {
        this.owner.internalUnregisterDescriptor(this);
        this.initialized = false;
        this.producer = null;
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">
    private void checkChangeable(@NotNull final String propertyName) {
        if (isInitialized()) {
            throw new OutcomeTopicsConfigurationException("Descriptor of income topic " + getTopic() + " can't change property " + propertyName + " after initialization!");
        }
    }

    /**
     * @param messageMode Режим данных в очереди: Пообъектно и пакетно.
     * @return this.
     */
    public @NotNull AbstractOutcomeTopicUploadingDescriptor setMessageMode(@NotNull final TopicMessageMode messageMode) {
        checkChangeable("messageMode");
        this.messageMode = messageMode;
        return this;
    }

    /**
     * @param serializeMode Режим сериализации: Строки или Байты.
     * @return this.
     */
    public @NotNull AbstractOutcomeTopicUploadingDescriptor setSerializeMode(@NotNull final SerializeMode serializeMode) {
        checkChangeable("serializeMode");
        this.serializeMode = serializeMode;
        return this;
    }

    /**
     * Установка приоритета у топика.
     * @param priority приоритет.
     * @return this.
     */
    public @NotNull AbstractOutcomeTopicUploadingDescriptor setPriority(final int priority) {
        checkChangeable("priority");
        this.priority = priority;
        return this;
    }

    /**
     * @return Количество Header-ов для отправляемых сообщений.
     */
    public int getDescriptorHeadersSize() {
        return this.descriptorHeaders.size();
    }

    /**
     * @return Список Header-ов для отправляемых сообщений.
     */
    @NotNull
    public Iterable<Header> getDescriptorHeaders() {
        return this.descriptorHeaders;
    }

    /**
     * Заменить список всех Header-ов на целевой набор.
     * @param headers целевой список Header-ов.
     * @return this.
     */
    @SuppressWarnings("UnusedReturnValue")
    @NotNull
    public final AbstractOutcomeTopicUploadingDescriptor setDescriptorHeaders(@NotNull final Iterable<Header> headers) {
        this.descriptorHeaders.clear();
        return addDescriptorHeaders(headers);
    }

    @Override
    @NotNull
    public AbstractOutcomeTopicUploadingDescriptor addDescriptorHeaders(@NotNull final Iterable<Header> headers) {
        headers.forEach(this.descriptorHeaders::add);
        return this;
    }

    /**
     * Добавление одного Header-а (если header с таким ключом уже есть, то замена).
     * @param header добавляемый Header.
     * @return this.
     */
    @NotNull
    public final AbstractOutcomeTopicUploadingDescriptor addDescriptorHeader(Header header) {
        final var index = this.descriptorHeaders.indexOf(header);
        if (index >= 0) {
            this.descriptorHeaders.set(index, header);
        } else {
            this.descriptorHeaders.add(header);
        }
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
