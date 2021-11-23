package ru.gx.kafka.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.jetbrains.annotations.NotNull;
import ru.gx.channels.AbstractOutcomeChannelDescriptor;
import ru.gx.channels.SerializeMode;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.security.InvalidParameterException;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class KafkaOutcomeTopicLoadingDescriptor<O extends DataObject, P extends DataPackage<O>>
        extends AbstractOutcomeChannelDescriptor<O, P> {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    /**
     * Ограничение по количеству Record-ов, отправляемых за раз. Можно измениять после инициализации.
     */
    @Getter
    @Setter
    private int maxPackageSize = 100;

    /**
     * Producer - публикатор сообщений в Kafka
     */
    @Getter
    private Producer<Long, ?> producer;

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">

    public KafkaOutcomeTopicLoadingDescriptor(@NotNull final AbstractKafkaOutcomeTopicsConfiguration owner, @NotNull final String topic, final KafkaOutcomeTopicLoadingDescriptorsDefaults defaults) {
        super(owner, topic, defaults);
        if (defaults != null) {
            this
                    .setMaxPackageSize(defaults.getMaxPackageSize());
        }
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @NotNull
    public KafkaOutcomeTopicLoadingDescriptor<O, P> init(@NotNull final Properties producerProperties) throws InvalidParameterException {
        if (this.getSerializeMode() == SerializeMode.JsonString) {
            this.producer = new KafkaProducer<Long, String>(producerProperties);
        } else {
            this.producer = new KafkaProducer<Long, byte[]>(producerProperties);
        }
        super.init();
        return this;
    }

    @Override
    @NotNull
    public KafkaOutcomeTopicLoadingDescriptor<O, P> init() throws InvalidParameterException {
        return init(this.getOwner().getDescriptorsDefaults().getProducerProperties());
    }

    @NotNull
    public KafkaOutcomeTopicLoadingDescriptor<O, P> unInit() {
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
