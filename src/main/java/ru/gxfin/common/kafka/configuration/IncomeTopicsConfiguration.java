package ru.gxfin.common.kafka.configuration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import ru.gxfin.common.data.AbstractMemoryRepository;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEventsFactory;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;
import ru.gxfin.common.kafka.loader.PartitionOffset;

import java.util.List;
import java.util.Properties;

/**
 * Интерфейс конфигурации обработки входящих очередей.
 */
@SuppressWarnings({"unused", "rawtypes"})
public interface IncomeTopicsConfiguration extends ObjectsLoadedFromIncomeTopicEventsFactory {
    /**
     * Полчение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    IncomeTopicLoadingDescriptor get(String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param priority          Приоритет очереди.
     * @param topic             Имя топика очереди.
     * @param consumer          Объект-получатель.
     * @param memoryRepository  Репозиторий, в который будут загружены входящие объекты.
     * @param mode              Режим данных в очереди: Пообъектно и пакетно.
     * @return this.
     */
    IncomeTopicsConfiguration register(
            int priority,
            String topic,
            Consumer consumer,
            List<TopicPartition> topicPartitions,
            AbstractMemoryRepository memoryRepository,
            TopicMessageMode mode,
            Class<? extends ObjectsLoadedFromIncomeTopicEvent> onLoadedEventClass
    );

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
    IncomeTopicsConfiguration register(
            int priority,
            String topic,
            AbstractMemoryRepository memoryRepository,
            TopicMessageMode mode,
            Class<? extends ObjectsLoadedFromIncomeTopicEvent> onLoadedEventClass,
            Properties consumerProperties,
            int... partitions
    );

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    IncomeTopicsConfiguration register(IncomeTopicLoadingDescriptor item);

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
    IncomeTopicsConfiguration unregister(String topic);

    /**
     * @return Количество приоритетов.
     */
    int prioritiesCount();

    /**
     * Получение списка описателей обработчиков очередей по приоритету.
     *
     * @param priority Приоритет.
     * @return Список описателей обработчиков.
     */
    Iterable<IncomeTopicLoadingDescriptor> getByPriority(int priority);

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    Iterable<IncomeTopicLoadingDescriptor> getAll();

    /**
     * Требование о смещении Offset-ов на начало для всех Topic-ов и всех Partition-ов.
     */
    void seekAllToBegin();

    /**
     * Требование о смещении Offset-ов на конец для всех Topic-ов и всех Partition-ов.
     */
    void seekAllToEnd();

    /**
     * Требование о смещении Offset-ов на начало для всех Partition-ов для заданного Topic-а.
     * @param topic Топик, для которого требуется сместить смещения.
     */
    void seekTopicAllPartitionsToBegin(String topic);

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
    * @param topic Топик, для которого требуется сместить смещения.
     */
    void seekTopicAllPartitionsToEnd(String topic);

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     * @param topic             Топик, для которого требуется сместить смещения.
     * @param partitionOffsets  Смещения (для каждого Partition-а свой Offset).
     */
    void seekTopic(String topic, Iterable<PartitionOffset> partitionOffsets);
}
