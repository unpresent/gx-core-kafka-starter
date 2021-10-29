package ru.gx.kafka.load;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.security.InvalidParameterException;
import java.util.Properties;

/**
 * Описатель обработчика одной очереди.
 */
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RawDataIncomeTopicLoadingDescriptor extends AbstractIncomeTopicLoadingDescriptor {

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @param consumerProperties Свойства consumer-а, который будет создан.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "unused"})
    @NotNull
    public RawDataIncomeTopicLoadingDescriptor init(@NotNull final Properties consumerProperties) throws InvalidParameterException {
        super.init(consumerProperties);
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    @Override
    @NotNull
    public RawDataIncomeTopicLoadingDescriptor unInit() {
        super.unInit();
        return this;
    }

    @SuppressWarnings({"unused"})
    protected RawDataIncomeTopicLoadingDescriptor(@NotNull final AbstractIncomeTopicsConfiguration owner, @NotNull final String topic, @Nullable final IncomeTopicLoadingDescriptorsDefaults defaults) {
        super(owner, topic, defaults);
    }
}
