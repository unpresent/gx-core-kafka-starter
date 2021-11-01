package ru.gx.kafka.upload;

import org.jetbrains.annotations.NotNull;

public interface OutcomeTopicsUploadingDescriptorsFactory {
    <C extends OutcomeTopicUploadingDescriptor>
    C create(
            @NotNull final OutcomeTopicsConfiguration owner,
            @NotNull final String topic,
            @NotNull final OutcomeTopicUploadingDescriptorsDefaults descriptorsDefaults,
            @NotNull final Class<C> descriptorClass
    );
}
