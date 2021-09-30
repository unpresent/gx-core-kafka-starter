package ru.gx.kafka.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;

@Getter
@Setter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class IncomeTopicLoadingStatistics {
    private int loadedPackagesCount;
    private int loadedObjectsCount;
    private int changesCount;
    private int insertedCount;
    private int updatedCount;
    private int replacedCount;

    private long loadedFromKafkaMs;
    private long prepareChangesMs;
    private long onLoadingEventMs;
    private long loadedToRepositoryMs;
    private long onLoadedEventMs;

    @NotNull
    public IncomeTopicLoadingStatistics reset() {
        this.loadedPackagesCount = -1;
        this.loadedObjectsCount = -1;
        this.changesCount = 0;
        this.insertedCount = 0;
        this.updatedCount = 0;
        this.replacedCount = 0;

        this.loadedFromKafkaMs = 0;
        this.prepareChangesMs = 0;
        this.onLoadingEventMs = 0;
        this.loadedToRepositoryMs = 0;
        this.onLoadedEventMs = 0;

        return this;
    }
}
