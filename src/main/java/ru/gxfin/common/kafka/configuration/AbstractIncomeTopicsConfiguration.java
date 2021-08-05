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
import ru.gxfin.common.kafka.loader.PartitionOffset;

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

    /**
     * Полчение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    @Override
    public IncomeTopicLoadingDescriptor get(String topic) {
        return this.topics.get(topic);
    }

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
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

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param priority         Приоритет очереди.
     * @param topic            Имя топика очереди.
     * @param consumer         Объект-получатель.
     * @param memoryRepository Репозиторий, в который будут загружены входящие объекты.
     * @param mode             Режим данных в очереди: Пообъектно и пакетно.
     * @return this.
     */
    @Override
    public IncomeTopicsConfiguration register(
            int priority,
            String topic,
            Consumer consumer,
            List<TopicPartition> topicPartitions,
            AbstractMemoryRepository memoryRepository,
            TopicMessageMode mode,
            Class<? extends ObjectsLoadedFromIncomeTopicEvent> onLoadedEventClass
    ) {
        return register(new IncomeTopicLoadingDescriptor(topic, priority, consumer, topicPartitions, memoryRepository, mode, onLoadedEventClass));
    }

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param priority           Приоритет очереди.
     * @param topic              Имя топика очереди.
     * @param memoryRepository   Репозиторий, в который будут загружены входящие объекты.
     * @param mode               Режим данных в очереди: Пообъектно и пакетно.
     * @param consumerProperties Свойства consumer-а.
     * @param partitions         Разделы в топике.
     * @return this.
     */
    @Override
    public IncomeTopicsConfiguration register(
            int priority,
            String topic,
            AbstractMemoryRepository memoryRepository,
            TopicMessageMode mode,
            Class<? extends ObjectsLoadedFromIncomeTopicEvent> onLoadedEventClass,
            Properties consumerProperties,
            int... partitions
    ) {
        final List<Integer> partitionsList = new ArrayList<>();
        final List<TopicPartition> topicPartitions = new ArrayList<>();
        Arrays.stream(partitions).forEach(p -> {
            topicPartitions.add(new TopicPartition(topic, p));
            partitionsList.add(p);
        });
        final var consumer = new KafkaConsumer(consumerProperties);
        consumer.assign(topicPartitions);
        return register(new IncomeTopicLoadingDescriptor(topic, priority, consumer, partitionsList, memoryRepository, mode, onLoadedEventClass));
    }

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
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

    /**
     * @return Количество приоритетов.
     */
    @Override
    public int prioritiesCount() {
        return this.priorities.size();
    }

    /**
     * Получение списка описателей обработчиков очередей по приоритету.
     *
     * @param priority Приоритет.
     * @return Список описателей обработчиков.
     */
    @Override
    public Iterable<IncomeTopicLoadingDescriptor> getByPriority(int priority) {
        return this.priorities.get(priority);
    }

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @Override
    public Iterable<IncomeTopicLoadingDescriptor> getAll() {
        return this.topics.values();
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

    /**
     * Требование о смещении Offset-ов на начало для всех Topic-ов и всех Partition-ов.
     */
    @Override
    public void seekAllToBegin() {
        this.topics.values().forEach(topicDescriptor ->
                topicDescriptor
                        .getConsumer()
                        .seekToBeginning(topicDescriptor.getTopicPartitions()));
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Topic-ов и всех Partition-ов.
     */
    @Override
    public void seekAllToEnd() {
        this.topics.values().forEach(topicDescriptor ->
                topicDescriptor
                        .getConsumer()
                        .seekToEnd(topicDescriptor.getTopicPartitions()));
    }

    /**
     * Требование о смещении Offset-ов на начало для всех Partition-ов для заданного Topic-а.
     *
     * @param topic Топик, для которого требуется сместить смещения.
     */
    @Override
    public void seekTopicAllPartitionsToBegin(String topic) {
        final var topicDescriptor = this.get(topic);
        topicDescriptor.getConsumer().seekToBeginning(topicDescriptor.getTopicPartitions());
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
     *
     * @param topic Топик, для которого требуется сместить смещения.
     */
    @Override
    public void seekTopicAllPartitionsToEnd(String topic) {
        final var topicDescriptor = this.get(topic);
        topicDescriptor.getConsumer().seekToEnd(topicDescriptor.getTopicPartitions());
    }

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     *
     * @param topic            Топик, для которого требуется сместить смещения.
     * @param partitionOffsets Смещения (для каждого Partition-а свой Offset).
     */
    @Override
    public void seekTopic(String topic, Iterable<PartitionOffset> partitionOffsets) {
        final var topicDescriptor = this.get(topic);
        topicDescriptor
                .getPartitionOffsets()
                .forEach((k, v) -> topicDescriptor.getConsumer().seek(new TopicPartition(topic, (int) k), (long) v));
    }
}
