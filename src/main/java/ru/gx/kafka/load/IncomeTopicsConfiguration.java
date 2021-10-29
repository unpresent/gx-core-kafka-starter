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
     *
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
    IncomeTopicLoadingDescriptor get(@NotNull final String topic);

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди. Если не найден, то возвращается null.
     */
    @Nullable
    IncomeTopicLoadingDescriptor tryGet(@NotNull final String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param topic           Топик, для которого создается описатель.
     * @param descriptorClass Класс описателя.
     * @return this.
     */
    @NotNull
    <D extends IncomeTopicLoadingDescriptor>
    D newDescriptor(@NotNull final String topic, Class<D> descriptorClass) throws InvalidParameterException;

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
    Iterable<IncomeTopicLoadingDescriptor> getByPriority(int priority);

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @NotNull
    Iterable<IncomeTopicLoadingDescriptor> getAll();
}
