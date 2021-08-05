package ru.gxfin.common.kafka.loader;

import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import ru.gxfin.common.data.DataMemoryRepository;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Описатель обработчика одной очереди.
 */
public class IncomeTopicLoadingDescriptor<O extends DataObject> {
    /**
     * Имя топика очереди.
     */
    @Getter
    private final String topic;

    /**
     * Приоритет, с которым надо обрабтавать очередь.
     * 0 - высший.
     * > 0 - менее приоритетный.
     */
    @Getter
    private final int priority;

    /**
     * Объект-получатель сообщений.
     */
    @SuppressWarnings("rawtypes")
    @Getter
    private final Consumer consumer;

    /**
     * Список смещений для каждого Partition-а
     */
    @Getter
    private final Map<Integer, Long> partitionOffsets = new HashMap<>();

    /**
     * Репозиторий, в который будут загружены входящие объекты.
     */
    @Getter
    private final DataMemoryRepository<O, DataPackage<O>> memoryRepository;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Getter
    private final TopicMessageMode messageMode;

    @Getter
    private final Class<? extends ObjectsLoadedFromIncomeTopicEvent<O>> onLoadedEventClass;

    public void setDeserializedPartitionOffset(int partition, long offset) {
        this.partitionOffsets.put(partition, offset);
    }

    public String getDeserializedPartitionsOffsetsForLog() {
        StringBuilder result = new StringBuilder();
        for (final var p : this.partitionOffsets.keySet()) {
            if (result.length() > 0) {
                result.append("; ");
            }
            result
                    .append(p.toString())
                    .append(':')
                    .append(this.partitionOffsets.get(p));
        }
        return result.toString();
    }

    public List<TopicPartition> getTopicPartitions() {
        final var result = new ArrayList<TopicPartition>();
        this.getPartitionOffsets().keySet().forEach(p -> result.add(new TopicPartition(getTopic(), p)));
        return result;
    }

    @SuppressWarnings("rawtypes")
    public IncomeTopicLoadingDescriptor(
            String topic,
            int priority,
            Consumer consumer,
            Iterable<Integer> partitions,
            DataMemoryRepository<O, DataPackage<O>> memoryRepository,
            TopicMessageMode messageMode,
            Class<? extends ObjectsLoadedFromIncomeTopicEvent<O>> onLoadedEventClass
    ) {
        this.topic = topic;
        this.priority = priority;
        this.consumer = consumer;
        partitions.forEach(p -> this.getPartitionOffsets().put(p, (long) -1));
        this.memoryRepository = memoryRepository;
        this.messageMode = messageMode;
        this.onLoadedEventClass = onLoadedEventClass;
    }
}
