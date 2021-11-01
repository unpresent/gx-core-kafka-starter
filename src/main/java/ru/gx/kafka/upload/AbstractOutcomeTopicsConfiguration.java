package ru.gx.kafka.upload;

import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.load.IncomeTopicLoadingDescriptor;
import ru.gx.kafka.load.IncomeTopicsConfigurationException;
import ru.gx.kafka.load.IncomeTopicsLoadingDescriptorsFactory;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

public abstract class AbstractOutcomeTopicsConfiguration implements OutcomeTopicsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Список описателей с группировкой по топикам.
     */
    @NotNull
    private final Map<String, OutcomeTopicUploadingDescriptor> topics = new HashMap<>();

    @Getter
    @NotNull
    private final OutcomeTopicUploadingDescriptorsDefaults descriptorsDefaults = new OutcomeTopicUploadingDescriptorsDefaults();

    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private OutcomeTopicsUploadingDescriptorsFactory descriptorsFactory;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractOutcomeTopicsConfiguration() {
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Реализация OutcomeTopicsConfiguration">
    @Override
    public boolean contains(@NotNull String topic) {
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
    public @NotNull <O extends DataObject, P extends DataPackage<O>>
    StandardOutcomeTopicUploadingDescriptor<O, P> get(@NotNull String topic) {
        final var result = (StandardOutcomeTopicUploadingDescriptor<O, P>) this.topics.get(topic);
        if (result == null) {
            throw new OutcomeTopicsConfigurationException("Can't find description for topic " + topic);
        }
        return result;
    }

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public <O extends DataObject, P extends DataPackage<O>>
    StandardOutcomeTopicUploadingDescriptor<O, P> tryGet(@NotNull String topic) {
        return (StandardOutcomeTopicUploadingDescriptor<O, P>) this.topics.get(topic);
    }


    /**
     * Создание описателя публикатора одной очереди.
     *
     * @param topic           Топик, для которого создается описатель.
     * @param descriptorClass Класс описателя.
     * @return this.
     */
    @Override
    @NotNull
    public
    <D extends OutcomeTopicUploadingDescriptor>
    D newDescriptor(@NotNull final String topic, Class<D> descriptorClass) throws InvalidParameterException {
        if (contains(topic)) {
            throw new OutcomeTopicsConfigurationException("Topic '" + topic + "' already registered!");
        }
        if (getDescriptorsFactory() == null) {
            throw new OutcomeTopicsConfigurationException("DescriptorsFactory doesn't set!");
        }
        return getDescriptorsFactory().create(this, topic, getDescriptorsDefaults(), descriptorClass);
    }

    /**
     * Регистрация описателя публикатора одной очереди.
     *
     * @param descriptor Описатель публикатора одной очереди.
     */
    protected void internalRegisterDescriptor(@NotNull final OutcomeTopicUploadingDescriptor descriptor) throws InvalidParameterException {
        if (contains(descriptor.getTopic())) {
            throw new OutcomeTopicsConfigurationException("Topic " + descriptor.getTopic() + " already registered!");
        }
        if (!descriptor.isInitialized()) {
            throw new OutcomeTopicsConfigurationException("Descriptor of topic '" + descriptor.getTopic() + "' doesn't initialized!");
        }

        topics.put(descriptor.getTopic(), descriptor);
    }

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param descriptor Имя топика очереди.
     */
    protected void internalUnregisterDescriptor(@NotNull final OutcomeTopicUploadingDescriptor descriptor) {
        final var topic = descriptor.getTopic();
        if (!this.topics.containsValue(descriptor) || !this.topics.containsKey(topic)) {
            throw new OutcomeTopicsConfigurationException("Topic " + topic + " not registered!");
        }
        if (!descriptor.equals(this.get(topic))) {
            throw new OutcomeTopicsConfigurationException("Descriptor by name " + topic + " not equal descriptor by parameter!");
        }

        this.topics.remove(topic);
    }

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @Override
    public @NotNull Iterable<OutcomeTopicUploadingDescriptor> getAll() {
        return this.topics.values();
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
