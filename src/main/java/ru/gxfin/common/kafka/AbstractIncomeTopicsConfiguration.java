package ru.gxfin.common.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import ru.gxfin.common.data.AbstractMemoryRepository;

import java.util.*;

@Slf4j
@SuppressWarnings("unused")
public abstract class AbstractIncomeTopicsConfiguration implements IncomeTopicsConfiguration {
    private final List<List<IncomeTopic2MemoryRepository>> priorities = new ArrayList<>();
    private final Map<String, IncomeTopic2MemoryRepository> topics = new HashMap<>();

    @SuppressWarnings("unused")
    protected AbstractIncomeTopicsConfiguration() {
        super();
    }

    @Override
    public IncomeTopic2MemoryRepository get(String topic) {
        return this.topics.get(topic);
    }

    @Override
    public AbstractIncomeTopicsConfiguration register(IncomeTopic2MemoryRepository item) {
        if (this.topics.containsKey(item.getTopic())) {
            throw new IncomeTopicsConsumingException("Topic " + item.getTopic() + " already registered!");
        }

        final var priority = item.getPriority();
        while (priorities.size() <= priority) {
            priorities.add(new ArrayList<>());
        }

        final var itemsList = priorities.get(priority);
        itemsList.add(item);

        topics.put(item.getTopic(), item);

        return this;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IncomeTopicsConfiguration register(int priority, String topic, Consumer consumer, AbstractMemoryRepository memoryRepository, TopicMessageMode mode) {
        return register(new IncomeTopic2MemoryRepository(topic, priority, consumer, memoryRepository, mode));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IncomeTopicsConfiguration register(int priority, String topic, AbstractMemoryRepository memoryRepository, TopicMessageMode mode, Properties consumerProperties, int... partitions) {
        final var consumer = defineSimpleTopicConsumer(consumerProperties, topic, partitions);
        return register(new IncomeTopic2MemoryRepository(topic, priority, consumer, memoryRepository, mode));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public Consumer defineSimpleTopicConsumer(Properties properties, String topic, int... partitions) {
        final var result = new KafkaConsumer(properties);
        final List<TopicPartition> topicPartitions = new ArrayList<>();
        for (var p : partitions) {
            topicPartitions.add(new TopicPartition(topic, p));
        }
        result.assign(topicPartitions);
        return result;
    }

    @Override
    public AbstractIncomeTopicsConfiguration unregister(String topic) {
        final var item = this.topics.get(topic);
        if (item == null) {
            throw new IncomeTopicsConsumingException("Topic " + topic + " not registered!");
        }

        this.topics.remove(topic);
        for (var pList : this.priorities) {
            if (pList.remove(item)) {
                break;
            }
        }

        return this;
    }

    @Override
    public int prioritiesCount() {
        return this.priorities.size();
    }

    @Override
    public Iterable<IncomeTopic2MemoryRepository> getByPriority(int priority) {
        return this.priorities.get(priority);
    }
}
