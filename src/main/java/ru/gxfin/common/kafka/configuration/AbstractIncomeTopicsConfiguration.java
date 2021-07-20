package ru.gxfin.common.kafka.configuration;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import ru.gxfin.common.data.AbstractMemoryRepository;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.IncomeTopicsConsumingException;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.events.AbstractObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEventsFactory;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

import java.util.*;

@Slf4j
@SuppressWarnings("unused")
public abstract class AbstractIncomeTopicsConfiguration
        implements IncomeTopicsConfiguration {

    private final List<List<IncomeTopicLoadingDescriptor>> priorities = new ArrayList<>();

    private final Map<String, IncomeTopicLoadingDescriptor> topics = new HashMap<>();

    /**
     * Режим получения объекта-события
     */
    @Getter
    private final ObjectsLoadedFromIncomeTopicEventsFactory.GettingMode eventsGettingMode;

    /**
     * Объект-событие, который предоставляется в режиме {@link GettingMode#Singleton}.
     */
    @SuppressWarnings("rawtypes")
    @Getter(AccessLevel.PROTECTED)
    private final Map<Class<ObjectsLoadedFromIncomeTopicEvent>, AbstractObjectsLoadedFromIncomeTopicEvent> events;

    @SuppressWarnings("unused")
    protected AbstractIncomeTopicsConfiguration(ObjectsLoadedFromIncomeTopicEventsFactory.GettingMode eventsGettingMode) {
        super();
        this.eventsGettingMode = eventsGettingMode;
        this.events = getEventsGettingMode() == GettingMode.Singleton ? new HashMap<>() : null;
    }

    @SuppressWarnings("rawtypes")
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

    @SuppressWarnings("rawtypes")
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

    @SuppressWarnings({"rawtypes", "unchecked"})
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

    @SuppressWarnings("rawtypes")
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
    @SuppressWarnings("rawtypes")
    @Override
    public AbstractObjectsLoadedFromIncomeTopicEvent getOrCreateEvent(Class<ObjectsLoadedFromIncomeTopicEvent> eventClass, Object source, IncomeTopicLoadingDescriptor loadingDescriptor, Iterable<DataObject> objects) {
        final var result = getEventsGettingMode() == GettingMode.New
                ? internalCreateEventInstance(eventClass)
                : this.events.get(eventClass);
        result.reset(source, loadingDescriptor, objects);
        return result;
    }

    /**
     * @return Новый пустой экземпляр объекта-события.
     */
    @SuppressWarnings("rawtypes")
    protected abstract AbstractObjectsLoadedFromIncomeTopicEvent internalCreateEventInstance(Class<ObjectsLoadedFromIncomeTopicEvent> eventClass);
}
