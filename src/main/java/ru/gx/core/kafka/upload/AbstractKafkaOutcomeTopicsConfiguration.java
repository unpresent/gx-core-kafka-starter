package ru.gx.core.kafka.upload;

import io.micrometer.core.instrument.MeterRegistry;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelHandlerDescriptor;

public abstract class AbstractKafkaOutcomeTopicsConfiguration extends AbstractChannelsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractKafkaOutcomeTopicsConfiguration(
            @NotNull final String configurationName,
            @NotNull final MeterRegistry meterRegistry
    ) {
        super(ChannelDirection.Out, configurationName, meterRegistry);
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
        return (KafkaOutcomeTopicUploadingDescriptorsDefaults) super.getDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
