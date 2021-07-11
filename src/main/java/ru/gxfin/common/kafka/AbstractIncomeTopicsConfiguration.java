package ru.gxfin.common.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import ru.gxfin.common.data.AbstractMemRepo;

import java.util.*;

/**
 * Базовая реализация конфигурации обработки входящих очередей.
 */
@Slf4j
public abstract class AbstractIncomeTopicsConfiguration implements IncomeTopicsConfiguration {
    /**
     * Список описателей по приоритету.
     * Внешний список - список приоритетов.
     * Внутренние списки - списки описателей одного приоритета.
     */
    private final List<List<IncomeTopic2MemRepo>> priorities = new ArrayList<>();

    /**
     * Список описателей по топику.
     */
    private final Map<String, IncomeTopic2MemRepo> topics = new HashMap<>();

    protected AbstractIncomeTopicsConfiguration() {
        super();
    }

    /**
     * Полчение описателя обработчика по топику.
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    @Override
    public IncomeTopic2MemRepo get(String topic) {
        return this.topics.get(topic);
    }

    /**
     * Регистрация описателя обработчика одной очереди.
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    @Override
    public AbstractIncomeTopicsConfiguration register(IncomeTopic2MemRepo item) {
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
    public IncomeTopicsConfiguration register(int priority, String topic, Consumer consumer, AbstractMemRepo memRepo, TopicMessageMode mode) {
        return register(new IncomeTopic2MemRepo(topic, priority, consumer, memRepo, mode));
    }

    @Override
    public IncomeTopicsConfiguration register(int priority, String topic, AbstractMemRepo memRepo, TopicMessageMode mode, Properties consumerProperties, int... partitions) {
        final var consumer = defineSimpleTopicConsumer(consumerProperties, topic, partitions);
        return register(new IncomeTopic2MemRepo(topic, priority, consumer, memRepo, mode));
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

    /**
     * Дерегистрация обработчика очереди.
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
     * @param priority Приоритет.
     * @return Список описателей обработчиков.
     */
    @Override
    public Iterable<IncomeTopic2MemRepo> getByPriority(int priority) {
        return this.priorities.get(0);
    }
}
