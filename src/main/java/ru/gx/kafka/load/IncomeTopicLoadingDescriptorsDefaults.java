package ru.gx.kafka.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.kafka.SerializeMode;
import ru.gx.kafka.TopicMessageMode;

import java.util.Properties;

@Getter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class IncomeTopicLoadingDescriptorsDefaults {
    @Setter
    @NotNull
    private LoadingMode loadingMode = LoadingMode.Auto;

    @Setter
    @NotNull
    private TopicMessageMode topicMessageMode = TopicMessageMode.Object;

    @Setter
    @NotNull
    private SerializeMode serializeMode = SerializeMode.String;

    @Setter
    @Nullable
    private LoadingFiltering loadingFiltering;

    @Getter
    @Setter
    @NotNull
    private Properties consumerProperties;

    private int[] partitions = new int[]{0};

    @SuppressWarnings("unused")
    public IncomeTopicLoadingDescriptorsDefaults setPartitions(int... partitions) {
        this.partitions = partitions;
        return this;
    }
}
