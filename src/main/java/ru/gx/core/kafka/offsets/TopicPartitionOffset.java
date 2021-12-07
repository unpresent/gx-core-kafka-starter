package ru.gx.core.kafka.offsets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class TopicPartitionOffset extends PartitionOffset {
    @Getter
    private final String topic;

    @JsonCreator
    public TopicPartitionOffset(
            @JsonProperty("topic") @NotNull final String topic,
            @JsonProperty("partition") final int partition,
            @JsonProperty("offset") final long offset
    ) {
        super(partition, offset);
        this.topic = topic;
    }
}
