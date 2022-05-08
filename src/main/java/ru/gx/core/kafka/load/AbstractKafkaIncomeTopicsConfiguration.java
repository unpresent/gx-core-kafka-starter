package ru.gx.core.kafka.load;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelHandlerDescriptor;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;

public abstract class AbstractKafkaIncomeTopicsConfiguration extends AbstractChannelsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractKafkaIncomeTopicsConfiguration(@NotNull final String configurationName) {
        super(ChannelDirection.In, configurationName);
    }

    @Override
    protected KafkaIncomeTopicLoadingDescriptorsDefaults createChannelDescriptorsDefaults() {
        return new KafkaIncomeTopicLoadingDescriptorsDefaults();
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsConfiguration">
    @Override
    protected <M extends Message<? extends MessageBody>, D extends ChannelHandlerDescriptor<M>>
    boolean allowCreateDescriptor(@NotNull Class<D> descriptorClass) {
        return KafkaIncomeTopicLoadingDescriptor.class.isAssignableFrom(descriptorClass);
    }

    @Override
    public @NotNull KafkaIncomeTopicLoadingDescriptorsDefaults getDescriptorsDefaults() {
        return (KafkaIncomeTopicLoadingDescriptorsDefaults) super.getDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
