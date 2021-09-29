package ru.gx.kafka.loader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class PartitionOffset implements Serializable {
    @Getter
    private final int partition;

    @Getter
    @Setter
    private long offset;

    public PartitionOffset(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }
}
