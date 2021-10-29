package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;

import java.security.InvalidParameterException;

public class StandardIncomeTopicsLoadingDescriptorsFactory implements IncomeTopicsLoadingDescriptorsFactory {
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <C extends IncomeTopicLoadingDescriptor> C create(
            @NotNull final IncomeTopicsConfiguration owner,
            @NotNull String topic,
            @NotNull IncomeTopicLoadingDescriptorsDefaults descriptorsDefaults,
            @NotNull Class<C> descriptorClass
    ) {
        if (!(owner instanceof AbstractIncomeTopicsConfiguration)) {
            throw new IncomeTopicsConfigurationException("Unsupported configuration class " + owner.getClass().getName());
        }
        final var localOwner = (AbstractIncomeTopicsConfiguration) owner;
        if (RawDataIncomeTopicLoadingDescriptor.class.equals(descriptorClass)) {
            return (C) new RawDataIncomeTopicLoadingDescriptor(localOwner, topic, descriptorsDefaults);
        } else if (StandardIncomeTopicLoadingDescriptor.class.equals(descriptorClass)) {
            return (C) new StandardIncomeTopicLoadingDescriptor(localOwner, topic, descriptorsDefaults);
        } else {
            throw new InvalidParameterException("Unknown descriptor class: " + descriptorClass.getName());
        }
    }
}
