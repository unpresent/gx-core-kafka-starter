package ru.gxfin.common.kafka.loader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

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
