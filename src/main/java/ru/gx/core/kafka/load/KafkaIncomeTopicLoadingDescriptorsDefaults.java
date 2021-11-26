package ru.gx.core.kafka.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.IncomeChannelDescriptorsDefaults;

import java.time.Duration;
import java.util.Properties;

@Getter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class KafkaIncomeTopicLoadingDescriptorsDefaults extends IncomeChannelDescriptorsDefaults {

    /**
     * Длительность, в течение которой ожидать данных из Топика.
     */
    @Setter
    @NotNull
    private Duration durationOnPoll = Duration.ofMillis(100);

    @Getter
    @Setter
    @NotNull
    private Properties consumerProperties;

    private int[] partitions = new int[]{0};

    protected KafkaIncomeTopicLoadingDescriptorsDefaults() {
        super();
    }

    @SuppressWarnings("unused")
    public KafkaIncomeTopicLoadingDescriptorsDefaults setPartitions(int... partitions) {
        this.partitions = partitions;
        return this;
    }
}
