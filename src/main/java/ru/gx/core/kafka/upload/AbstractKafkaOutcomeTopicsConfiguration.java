package ru.gx.core.kafka.upload;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelHandlerDescriptor;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;

public abstract class AbstractKafkaOutcomeTopicsConfiguration extends AbstractChannelsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractKafkaOutcomeTopicsConfiguration(@NotNull final String configurationName) {
        super(ChannelDirection.Out, configurationName);
    }

    @Override
    protected KafkaOutcomeTopicUploadingDescriptorsDefaults createChannelDescriptorsDefaults() {
        return new KafkaOutcomeTopicUploadingDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicsConfiguration">
    @Override
    protected <D extends ChannelHandlerDescriptor>
    boolean allowCreateDescriptor(@NotNull Class<D> descriptorClass) {
        return KafkaOutcomeTopicUploadingDescriptor.class.isAssignableFrom(descriptorClass);
    }

    @Override
    public @NotNull KafkaOutcomeTopicUploadingDescriptorsDefaults getDescriptorsDefaults() {
        return (KafkaOutcomeTopicUploadingDescriptorsDefaults)super.getDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
