package ru.gx.core.kafka.load;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDescriptor;
import ru.gx.core.channels.ChannelDirection;

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
    protected boolean allowCreateDescriptor(@NotNull Class<? extends ChannelDescriptor> descriptorClass) {
        return KafkaIncomeTopicLoadingDescriptor.class.isAssignableFrom(descriptorClass);
    }

    @Override
    public @NotNull KafkaIncomeTopicLoadingDescriptorsDefaults getDescriptorsDefaults() {
        return (KafkaIncomeTopicLoadingDescriptorsDefaults)super.getDescriptorsDefaults();
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
