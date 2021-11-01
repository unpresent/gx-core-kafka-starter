package ru.gx.kafka.load;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static lombok.AccessLevel.*;

public abstract class AbstractIncomeTopicsConfiguration implements IncomeTopicsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Список описателей сгруппированные по приоритетам.
     */
    @NotNull
    private final List<List<IncomeTopicLoadingDescriptor>> priorities = new ArrayList<>();

    /**
     * Список описателей с группировкой по топикам.
     */
    @NotNull
    private final Map<String, IncomeTopicLoadingDescriptor> topics = new HashMap<>();

    @Getter
    @NotNull
    private final IncomeTopicLoadingDescriptorsDefaults descriptorsDefaults;

    @NotNull
    private final String readerName;

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private IncomeTopicsLoadingDescriptorsFactory descriptorsFactory;

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
     *
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
    @Override
    @NotNull
    public IncomeTopicLoadingDescriptor get(@NotNull final String topic) {
        final var result = this.topics.get(topic);
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
    @Override
    @Nullable
    public IncomeTopicLoadingDescriptor tryGet(@NotNull final String topic) {
        return this.topics.get(topic);
    }

    /**
     * Создание описателя обработчика одной очереди.
     *
     * @param topic           Топик, для которого создается описатель.
     * @param descriptorClass Класс описателя.
     * @return this.
     */
    @Override
    @NotNull
    public
    <D extends IncomeTopicLoadingDescriptor>
    D newDescriptor(@NotNull final String topic, Class<D> descriptorClass) throws InvalidParameterException {
        if (contains(topic)) {
            throw new IncomeTopicsConfigurationException("Topic '" + topic + "' already registered!");
        }
        if (getDescriptorsFactory() == null) {
            throw new IncomeTopicsConfigurationException("DescriptorsFactory doesn't set!");
        }
        return getDescriptorsFactory().create(this, topic, getDescriptorsDefaults(), descriptorClass);
    }

    /**
     * Регистрация описателя одной очереди.
     *
     * @param descriptor Описатель топика, который надо зарегистрировать в списках описателей.
     */
    protected void internalRegisterDescriptor(@NotNull final IncomeTopicLoadingDescriptor descriptor) {
        if (contains(descriptor.getTopic())) {
            throw new IncomeTopicsConfigurationException("Topic '" + descriptor.getTopic() + "' already registered!");
        }
        if (!descriptor.isInitialized()) {
            throw new IncomeTopicsConfigurationException("Descriptor of topic '" + descriptor.getTopic() + "' doesn't initialized!");
        }

        final var priority = descriptor.getPriority();
        while (priorities.size() <= priority) {
            priorities.add(new ArrayList<>());
        }
        final var itemsList = priorities.get(priority);
        itemsList.add(descriptor);

        topics.put(descriptor.getTopic(), descriptor);
    }

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param descriptor Описатель топика очереди.
     */
    protected void internalUnregisterDescriptor(@NotNull final IncomeTopicLoadingDescriptor descriptor) {
        final var topic = descriptor.getTopic();
        if (!this.topics.containsValue(descriptor) || !this.topics.containsKey(topic)) {
            throw new IncomeTopicsConfigurationException("Topic " + topic + " not registered!");
        }
        if (!descriptor.equals(this.get(topic))) {
            throw new IncomeTopicsConfigurationException("Descriptor by name " + topic + " not equal descriptor by parameter!");
        }

        this.topics.remove(descriptor.getTopic());
        for (var pList : this.priorities) {
            if (pList.remove(descriptor)) {
                descriptor.unInit();
                break;
            }
        }
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
    public Iterable<IncomeTopicLoadingDescriptor> getByPriority(int priority) {
        return this.priorities.get(priority);
    }

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @Override
    @NotNull
    public Iterable<IncomeTopicLoadingDescriptor> getAll() {
        return this.topics.values();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
