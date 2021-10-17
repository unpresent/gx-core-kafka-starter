package ru.gx.kafka.load;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractIncomeTopicsConfiguration implements IncomeTopicsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Список описателей сгруппированные по приоритетам.
     */
    @NotNull
    private final List<List<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>>> priorities = new ArrayList<>();

    /**
     * Список описателей с группировкой по топикам.
     */
    @NotNull
    private final Map<String, IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> topics = new HashMap<>();

    @Getter
    @NotNull
    private final IncomeTopicLoadingDescriptorsDefaults descriptorsDefaults;

    @NotNull
    private final String readerName;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractIncomeTopicsConfiguration(@NotNull final String readerName) {
        this.readerName = readerName;
        this.descriptorsDefaults = new IncomeTopicLoadingDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsConfiguration">

    /**
     * @return Логическое имя читателя
     */
    @Override
    @NotNull
    public String getReaderName() {
        return this.readerName;
    }

    /**
     * Проверка регистрации описателя топика в конфигурации.
     * @param topic Топик.
     * @return true - описатель топика зарегистрирован.
     */
    @Override
    public boolean contains(@NotNull final String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    @SuppressWarnings("unchecked")
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    IncomeTopicLoadingDescriptor<O, P> get(@NotNull final String topic) {
        final var result = (IncomeTopicLoadingDescriptor<O, P>)this.topics.get(topic);
        if (result == null) {
            throw new IncomeTopicsConfigurationException("Can't find description for topic " + topic);
        }
        return result;
    }

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди. Если не найден, то возвращается null.
     */
    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public <O extends DataObject, P extends DataPackage<O>>
    IncomeTopicLoadingDescriptor<O, P> tryGet(@NotNull final String topic) {
        return (IncomeTopicLoadingDescriptor<O, P>) this.topics.get(topic);
    }

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>>
    IncomeTopicsConfiguration register(@NotNull final IncomeTopicLoadingDescriptor<O, P> item) throws InvalidParameterException {
        if (contains(item.getTopic())) {
            throw new IncomeTopicsConfigurationException("Topic " + item.getTopic() + " already registered!");
        }

        if (!item.isInitialized()) {
            final var props = getDescriptorsDefaults().getConsumerProperties();
            if (props == null) {
                throw new IncomeTopicsConfigurationException("Invalid null value getDescriptorsDefaults().getConsumerProperties()!");
            }
            item.init(props);
        }

        final var priority = item.getPriority();
        while (priorities.size() <= priority) {
            priorities.add(new ArrayList<>());
        }

        final var itemsList = priorities.get(priority);

        final var localItem = (IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>)item;
        itemsList.add(localItem);
        topics.put(item.getTopic(), localItem);

        return this;
    }

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
    @Override
    @NotNull
    public IncomeTopicsConfiguration unregister(@NotNull final String topic) {
        final var item = this.topics.get(topic);
        if (item == null) {
            throw new IncomeTopicsConfigurationException("Topic " + topic + " not registered!");
        }

        this.topics.remove(topic);
        for (var pList : this.priorities) {
            if (pList.remove(item)) {
                item.unInit();
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
    @Nullable
    public Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getByPriority(int priority) {
        return this.priorities.get(priority);
    }

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @Override
    @NotNull
    public Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getAll() {
        return this.topics.values();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
