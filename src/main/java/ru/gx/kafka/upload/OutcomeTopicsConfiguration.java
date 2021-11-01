package ru.gx.kafka.upload;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.load.IncomeTopicLoadingDescriptor;
import ru.gx.kafka.load.IncomeTopicsConfigurationException;

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
    StandardOutcomeTopicUploadingDescriptor<O, P> get(@NotNull final String topic);


    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди. Если не найден, то возвращается null.
     */
    @Nullable
    <O extends DataObject, P extends DataPackage<O>>
    StandardOutcomeTopicUploadingDescriptor<O, P> tryGet(@NotNull final String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param topic           Топик, для которого создается описатель.
     * @param descriptorClass Класс описателя.
     * @return this.
     */
    <D extends OutcomeTopicUploadingDescriptor>
    D newDescriptor(@NotNull final String topic, Class<D> descriptorClass) throws InvalidParameterException;

    /**
     * @return Настройки по умолчанию для новых описателей загрузки из топиков.
     */
    @NotNull
    OutcomeTopicUploadingDescriptorsDefaults getDescriptorsDefaults();

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @NotNull
    Iterable<OutcomeTopicUploadingDescriptor> getAll();
}
