package ru.gxfin.common.kafka.configuration;

import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;
import ru.gxfin.common.kafka.loader.PartitionOffset;

/**
 * Интерфейс конфигурации обработки входящих очередей.
 */
@SuppressWarnings({"unused", "rawtypes"})
public interface IncomeTopicsConfiguration {
    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    IncomeTopicLoadingDescriptor get(String topic);

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
     * @return Настройки по умолчанию для новых описателей загрузки из топиков.
     */
    IncomeTopicLoadingDescriptorsDefaults getDescriptorsDefaults();

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
