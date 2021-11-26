package ru.gx.core.kafka.offsets;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class TopicPartitionOffset extends PartitionOffset {
    @Getter
    private final String topic;

    public TopicPartitionOffset(@NotNull final String topic, final int partition, final long offset) {
        super(partition, offset);
        this.topic = topic;
    }
}
