package ru.gx.core.kafka.load;

import io.micrometer.core.instrument.MeterRegistry;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.channels.ChannelHandlerDescriptor;

public abstract class AbstractKafkaIncomeTopicsConfiguration extends AbstractChannelsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractKafkaIncomeTopicsConfiguration(
            @NotNull final String configurationName,
            @NotNull final MeterRegistry meterRegistry
    ) {
        super(ChannelDirection.In, configurationName, meterRegistry);
    }

    @Override
    protected KafkaIncomeTopicLoadingDescriptorsDefaults createChannelDescriptorsDefaults() {
        return new KafkaIncomeTopicLoadingDescriptorsDefaults();
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsConfiguration">
    @Override
    protected <D extends ChannelHandlerDescriptor>
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
