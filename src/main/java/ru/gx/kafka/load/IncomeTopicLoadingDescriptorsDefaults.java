package ru.gx.kafka.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
    private TopicMessageMode topicMessageMode = TopicMessageMode.OBJECT;

    @Setter
    @Nullable
    private LoadingFiltering loadingFiltering;

    @Setter
    @Nullable
    private Properties consumerProperties;

    private int[] partitions = new int[]{0};

    @SuppressWarnings("unused")
    protected IncomeTopicLoadingDescriptorsDefaults setPartitions(int... partitions) {
        this.partitions = partitions;
        return this;
    }
}
