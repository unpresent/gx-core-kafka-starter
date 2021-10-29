package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;

public interface IncomeTopicsLoadingDescriptorsFactory {
    <C extends IncomeTopicLoadingDescriptor>
    C create(
            @NotNull final IncomeTopicsConfiguration owner,
            @NotNull final String topic,
            @NotNull final IncomeTopicLoadingDescriptorsDefaults descriptorsDefaults,
            @NotNull final Class<C> descriptorClass
    );
}
