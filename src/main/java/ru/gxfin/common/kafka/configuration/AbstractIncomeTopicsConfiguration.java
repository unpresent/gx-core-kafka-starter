package ru.gxfin.common.kafka.configuration;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import ru.gxfin.common.kafka.IncomeTopicsConsumingException;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;
import ru.gxfin.common.kafka.loader.PartitionOffset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@SuppressWarnings({"unused", "rawtypes", "unchecked"})
public abstract class AbstractIncomeTopicsConfiguration implements IncomeTopicsConfiguration {

    private final List<List<IncomeTopicLoadingDescriptor>> priorities = new ArrayList<>();

    private final Map<String, IncomeTopicLoadingDescriptor> topics = new HashMap<>();

    @Getter
    private final IncomeTopicLoadingDescriptorsDefaults descriptorsDefaults;

    protected AbstractIncomeTopicsConfiguration() {
        super();
        this.descriptorsDefaults = new IncomeTopicLoadingDescriptorsDefaults();
    }

    /**
     * Получение описателя обработчика по топику.
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

        if (!item.isInitialized()) {
            item.init(getDescriptorsDefaults().getConsumerProperties());
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
