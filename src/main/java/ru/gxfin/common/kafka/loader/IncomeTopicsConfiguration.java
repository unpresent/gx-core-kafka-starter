package ru.gxfin.common.kafka.loader;

import org.jetbrains.annotations.NotNull;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;

/**
 * Интерфейс конфигурации обработки входящих очередей.
 */
@SuppressWarnings("unused")
public interface IncomeTopicsConfiguration {

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    <O extends DataObject, P extends DataPackage<O>> IncomeTopicLoadingDescriptor<O, P> get(@NotNull String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    <O extends DataObject, P extends DataPackage<O>> IncomeTopicsConfiguration register(@NotNull IncomeTopicLoadingDescriptor<O, P> item);

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
    IncomeTopicsConfiguration unregister(@NotNull String topic);

    /**
     * @return Настройки по умолчанию для новых описателей загрузки из топиков.
     */
    @NotNull IncomeTopicLoadingDescriptorsDefaults getDescriptorsDefaults();

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
    void seekTopicAllPartitionsToBegin(@NotNull String topic);

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
     *
     * @param topic Топик, для которого требуется сместить смещения.
     */
    void seekTopicAllPartitionsToEnd(@NotNull String topic);

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     *
     * @param topic            Топик, для которого требуется сместить смещения.
     * @param partitionOffsets Смещения (для каждого Partition-а свой Offset).
     */
    void seekTopic(@NotNull String topic, @NotNull Iterable<PartitionOffset> partitionOffsets);
}
