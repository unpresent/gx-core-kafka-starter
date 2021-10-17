package ru.gx.kafka.upload;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.offsets.PartitionOffset;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractOutcomeTopicsConfiguration implements OutcomeTopicsConfiguration {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Список описателей с группировкой по топикам.
     */
    @NotNull
    private final Map<String, OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> topics = new HashMap<>();

    @Getter
    @NotNull
    private final OutcomeTopicUploadingDescriptorsDefaults descriptorsDefaults = new OutcomeTopicUploadingDescriptorsDefaults();

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
    OutcomeTopicUploadingDescriptor<O, P> get(@NotNull String topic) {
        final var result = (OutcomeTopicUploadingDescriptor<O, P>) this.topics.get(topic);
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
    OutcomeTopicUploadingDescriptor<O, P> tryGet(@NotNull String topic) {
        return (OutcomeTopicUploadingDescriptor<O, P>) this.topics.get(topic);
    }

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    @Override
    public @NotNull <O extends DataObject, P extends DataPackage<O>>
    OutcomeTopicsConfiguration register(@NotNull OutcomeTopicUploadingDescriptor<O, P> item) throws InvalidParameterException {
        if (contains(item.getTopic())) {
            throw new OutcomeTopicsConfigurationException("Topic " + item.getTopic() + " already registered!");
        }

        if (!item.isInitialized()) {
            final var props = getDescriptorsDefaults().getProducerProperties();
            if (props == null) {
                throw new OutcomeTopicsConfigurationException("Invalid null value getDescriptorsDefaults().getProducerProperties()!");
            }
            item.init(props);
        }

        final var localItem = (OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>) item;
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
    public @NotNull OutcomeTopicsConfiguration unregister(@NotNull String topic) {
        if (!contains(topic)) {
            throw new OutcomeTopicsConfigurationException("Topic " + topic + " not registered!");
        }

        this.topics.remove(topic);
        return this;
    }

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @Override
    public @NotNull Iterable<OutcomeTopicUploadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getAll() {
        return this.topics.values();
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
