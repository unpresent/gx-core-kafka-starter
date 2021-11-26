package ru.gx.core.kafka.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.OutcomeChannelDescriptorsDefaults;

import java.util.Properties;

@SuppressWarnings("unused")
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class KafkaOutcomeTopicLoadingDescriptorsDefaults extends OutcomeChannelDescriptorsDefaults {

    /**
     * Свойства для создания Producer-а.
     */
    @Getter
    @Setter
    @NotNull
    private Properties producerProperties;

    /**
     * Максимальное количество объектов в пакете данных.
     */
    @Getter
    @Setter
    private int maxPackageSize = 100;

    protected KafkaOutcomeTopicLoadingDescriptorsDefaults() {
        super();
    }
}
