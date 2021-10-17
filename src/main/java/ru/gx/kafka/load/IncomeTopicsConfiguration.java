package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.security.InvalidParameterException;

/**
 * Интерфейс конфигурации обработки входящих очередей.
 */
@SuppressWarnings("unused")
public interface IncomeTopicsConfiguration {
    @NotNull
    String getReaderName();

    /**
     * Проверка регистрации описателя топика в конфигурации.
     * @param topic Топик.
     * @return true - описатель топика зарегистрирован.
     */
    boolean contains(@NotNull final String topic);

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>> IncomeTopicLoadingDescriptor<O, P> get(@NotNull final String topic);


    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди. Если не найден, то возвращается null.
     */
    @Nullable
    <O extends DataObject, P extends DataPackage<O>> IncomeTopicLoadingDescriptor<O, P> tryGet(@NotNull final String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>> IncomeTopicsConfiguration register(@NotNull final IncomeTopicLoadingDescriptor<O, P> item) throws InvalidParameterException;

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
    @NotNull
    IncomeTopicsConfiguration unregister(@NotNull final String topic);

    /**
     * @return Настройки по умолчанию для новых описателей загрузки из топиков.
     */
    @NotNull
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
    @Nullable
    Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getByPriority(int priority);

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @NotNull
    Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getAll();
}
