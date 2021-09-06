package ru.gxfin.common.kafka.loader;

import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;

/**
 * Интерфейс конфигурации обработки входящих очередей.
 */
public interface IncomeTopicsConfiguration {

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    <O extends DataObject, P extends DataPackage<O>> IncomeTopicLoadingDescriptor<O, P> get(String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    <O extends DataObject, P extends DataPackage<O>> IncomeTopicsConfiguration register(IncomeTopicLoadingDescriptor<O, P> item);

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
    Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getByPriority(int priority);

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @SuppressWarnings("unused")
    Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getAll();

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
     *
     * @param topic Топик, для которого требуется сместить смещения.
     */
    void seekTopicAllPartitionsToBegin(String topic);

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
     *
     * @param topic Топик, для которого требуется сместить смещения.
     */
    void seekTopicAllPartitionsToEnd(String topic);

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     *
     * @param topic            Топик, для которого требуется сместить смещения.
     * @param partitionOffsets Смещения (для каждого Partition-а свой Offset).
     */
    void seekTopic(String topic, Iterable<PartitionOffset> partitionOffsets);
}
