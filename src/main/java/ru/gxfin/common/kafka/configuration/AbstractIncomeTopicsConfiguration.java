package ru.gxfin.common.kafka.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.AbstractMemoryRepository;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.IncomeTopicsConsumingException;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

import java.util.*;

@Slf4j
@SuppressWarnings({"unused", "rawtypes", "unchecked"})
public abstract class AbstractIncomeTopicsConfiguration
        implements IncomeTopicsConfiguration {

    private final ApplicationContext context;

    private final List<List<IncomeTopicLoadingDescriptor>> priorities = new ArrayList<>();

    private final Map<String, IncomeTopicLoadingDescriptor> topics = new HashMap<>();

    protected AbstractIncomeTopicsConfiguration(ApplicationContext context) {
        super();
        this.context = context;
    }

    @Override
    public IncomeTopicLoadingDescriptor get(String topic) {
        return this.topics.get(topic);
    }

    @Override
    public AbstractIncomeTopicsConfiguration register(IncomeTopicLoadingDescriptor item) {
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

    @Override
    public IncomeTopicsConfiguration register(
            int priority,
            String topic,
            Consumer consumer,
            AbstractMemoryRepository memoryRepository,
            TopicMessageMode mode,
            Class<ObjectsLoadedFromIncomeTopicEvent> onLoadedEventClass
    ) {
        return register(new IncomeTopicLoadingDescriptor(topic, priority, consumer, memoryRepository, mode, onLoadedEventClass));
    }

    @Override
    public IncomeTopicsConfiguration register(
            int priority,
            String topic,
            AbstractMemoryRepository memoryRepository,
            TopicMessageMode mode,
            Class<ObjectsLoadedFromIncomeTopicEvent> onLoadedEventClass,
            Properties consumerProperties,
            int... partitions
    ) {
        final var consumer = defineSimpleTopicConsumer(consumerProperties, topic, partitions);
        return register(new IncomeTopicLoadingDescriptor(topic, priority, consumer, memoryRepository, mode, onLoadedEventClass));
    }

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
    public Iterable<IncomeTopicLoadingDescriptor> getByPriority(int priority) {
        return this.priorities.get(priority);
    }

    /**
     * Получение объекта-события, заполненного параметрами.
     *
     * @param source            Источник события.
     * @param loadingDescriptor Описатель загрузчика из topic-а.
     * @param objects           Прочитанные объекты при чтении из топика.
     * @return Объект-событие.
     */
    @Override
    public ObjectsLoadedFromIncomeTopicEvent getOrCreateEvent(Class<? extends ObjectsLoadedFromIncomeTopicEvent> eventClass, Object source, IncomeTopicLoadingDescriptor loadingDescriptor, Iterable<DataObject> objects) {
        final var result = this.context.getBean(eventClass);
        result.reset(source, loadingDescriptor, objects);
        return result;
    }
}
