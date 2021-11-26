package ru.gx.core.kafka.upload;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDescriptor;
import ru.gx.core.channels.ChannelDirection;

public abstract class AbstractKafkaOutcomeTopicsConfiguration extends AbstractChannelsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractKafkaOutcomeTopicsConfiguration(@NotNull final String configurationName) {
        super(ChannelDirection.Out, configurationName);
    }

    @Override
    protected KafkaOutcomeTopicLoadingDescriptorsDefaults createChannelDescriptorsDefaults() {
        return new KafkaOutcomeTopicLoadingDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicsConfiguration">
    @Override
    protected boolean allowCreateDescriptor(@NotNull Class<? extends ChannelDescriptor> descriptorClass) {
        return KafkaOutcomeTopicLoadingDescriptor.class.isAssignableFrom(descriptorClass);
    }

    @Override
    public @NotNull KafkaOutcomeTopicLoadingDescriptorsDefaults getDescriptorsDefaults() {
        return (KafkaOutcomeTopicLoadingDescriptorsDefaults)super.getDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
