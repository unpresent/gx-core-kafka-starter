package ru.gx.core.kafka.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.core.channels.AbstractOutcomeChannelHandlerDescriptor;
import ru.gx.core.channels.ChannelApiDescriptor;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;

import java.security.InvalidParameterException;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class KafkaOutcomeTopicUploadingDescriptor<M extends Message<? extends MessageBody>>
        extends AbstractOutcomeChannelHandlerDescriptor<M> {
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

    public KafkaOutcomeTopicUploadingDescriptor(
            @NotNull final AbstractKafkaOutcomeTopicsConfiguration owner,
            @NotNull final ChannelApiDescriptor<M> api,
            @Nullable final KafkaOutcomeTopicUploadingDescriptorsDefaults defaults
    ) {
        super(owner, api, defaults);
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @NotNull
    public KafkaOutcomeTopicUploadingDescriptor<M> init(
            @NotNull final Properties producerProperties
    ) throws InvalidParameterException {
        if (this.getApi().getSerializeMode() == SerializeMode.JsonString) {
            this.producer = new KafkaProducer<Long, String>(producerProperties);
        } else {
            this.producer = new KafkaProducer<Long, byte[]>(producerProperties);
        }
        super.init();
        return this;
    }

    @Override
    @NotNull
    public KafkaOutcomeTopicUploadingDescriptor<M> init() throws InvalidParameterException {
        return init(this.getOwner().getDescriptorsDefaults().getProducerProperties());
    }

    @NotNull
    public KafkaOutcomeTopicUploadingDescriptor<M> unInit() {
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
