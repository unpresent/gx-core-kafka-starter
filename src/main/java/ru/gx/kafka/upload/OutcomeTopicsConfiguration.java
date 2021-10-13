package ru.gx.kafka.upload;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.PartitionOffset;
import ru.gx.kafka.load.IncomeTopicLoadingDescriptor;
import ru.gx.kafka.load.IncomeTopicLoadingDescriptorsDefaults;

import java.security.InvalidParameterException;

/**
 * Интерфейс конфигурации обработки входящих очередей.
 */
@SuppressWarnings("unused")
public interface OutcomeTopicsConfiguration {
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
    <O extends DataObject, P extends DataPackage<O>>
    OutcomeTopicUploadingDescriptor<O, P> get(@NotNull final String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>>
    OutcomeTopicsConfiguration register(@NotNull final OutcomeTopicUploadingDescriptor<O, P> item) throws InvalidParameterException;

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
    @NotNull
    OutcomeTopicsConfiguration unregister(@NotNull final String topic);

    /**
     * @return Настройки по умолчанию для новых описателей загрузки из топиков.
     */
    @NotNull
    OutcomeTopicUploadingDescriptorsDefaults getDescriptorsDefaults();

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @NotNull
    Iterable<OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getAll();
}
