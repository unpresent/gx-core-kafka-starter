package ru.gx.kafka.upload;

import org.jetbrains.annotations.NotNull;

import java.security.InvalidParameterException;

public class StandardOutcomeTopicsUploadingDescriptorsFactory implements OutcomeTopicsUploadingDescriptorsFactory {
    @SuppressWarnings({"unchecked"})
    @Override
    public <C extends OutcomeTopicUploadingDescriptor> C create(
            @NotNull OutcomeTopicsConfiguration owner,
            @NotNull String topic,
            @NotNull OutcomeTopicUploadingDescriptorsDefaults descriptorsDefaults,
            @NotNull Class<C> descriptorClass
    ) {
        if (!(owner instanceof AbstractOutcomeTopicsConfiguration)) {
            throw new OutcomeTopicsConfigurationException("Unsupported configuration class " + owner.getClass().getName());
        }
        final var localOwner = (AbstractOutcomeTopicsConfiguration) owner;
        if (RawDataOutcomeTopicUploadingDescriptor.class.equals(descriptorClass)) {
            return (C) new RawDataOutcomeTopicUploadingDescriptor(localOwner, topic, descriptorsDefaults);
        } else if (StandardOutcomeTopicUploadingDescriptor.class.equals(descriptorClass)) {
            return (C) new StandardOutcomeTopicUploadingDescriptor<>(localOwner, topic, descriptorsDefaults);
        } else {
            throw new InvalidParameterException("Unknown descriptor class: " + descriptorClass.getName());
        }
    }
}
