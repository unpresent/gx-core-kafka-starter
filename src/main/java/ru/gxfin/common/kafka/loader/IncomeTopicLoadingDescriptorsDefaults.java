package ru.gxfin.common.kafka.loader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import ru.gxfin.common.kafka.TopicMessageMode;

import java.util.Properties;

@Getter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class IncomeTopicLoadingDescriptorsDefaults {
    @Setter
    private LoadingMode loadingMode = LoadingMode.Auto;

    @Setter
    private TopicMessageMode topicMessageMode = TopicMessageMode.OBJECT;

    @Getter
    @Setter
    private LoadingFiltering loadingFiltering;

    @Setter
    private Properties consumerProperties;

    private int[] partitions = new int[]{0};

    @SuppressWarnings("unused")
    public IncomeTopicLoadingDescriptorsDefaults setPartitions(int... partitions) {
        this.partitions = partitions;
        return this;
    }
}
