package ru.gx.core.kafka.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.core.channels.*;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;

import java.security.InvalidParameterException;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class KafkaOutcomeTopicUploadingDescriptor
        extends AbstractOutcomeChannelHandlerDescriptor {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Producer - публикатор сообщений в Kafka
     */
    @Getter
    private Producer<Long, ?> producer;

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">

    @SuppressWarnings("unused")
    public KafkaOutcomeTopicUploadingDescriptor(
            @NotNull final AbstractKafkaOutcomeTopicsConfiguration owner,
            @NotNull final ChannelApiDescriptor<? extends Message<? extends MessageBody>> api,
            @Nullable final KafkaOutcomeTopicUploadingDescriptorsDefaults defaults
    ) {
        super(owner, api, defaults);
    }

    @SuppressWarnings("unused")
    public KafkaOutcomeTopicUploadingDescriptor(
            @NotNull final ChannelsConfiguration owner,
            @NotNull final String channelName,
            @Nullable final OutcomeChannelDescriptorsDefaults defaults
    ) {
        super(owner, channelName, defaults);
        throw new NullPointerException("getApi() is null!");
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @NotNull
    public KafkaOutcomeTopicUploadingDescriptor init(
            @NotNull final Properties producerProperties
    ) throws InvalidParameterException {
        final var api = getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }

        if (api.getSerializeMode() == SerializeMode.JsonString) {
            this.producer = new KafkaProducer<Long, String>(producerProperties);
        } else {
            this.producer = new KafkaProducer<Long, byte[]>(producerProperties);
        }
        super.init();
        return this;
    }

    @Override
    @NotNull
    public KafkaOutcomeTopicUploadingDescriptor init() throws InvalidParameterException {
        return init(this.getOwner().getDescriptorsDefaults().getProducerProperties());
    }

    @NotNull
    public KafkaOutcomeTopicUploadingDescriptor unInit() {
        this.producer = null;
        super.unInit();
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">
    @Override
    @NotNull
    public AbstractKafkaOutcomeTopicsConfiguration getOwner() {
        return (AbstractKafkaOutcomeTopicsConfiguration)super.getOwner();
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
