package ru.gx.kafka.load;

import lombok.*;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.ApplicationContext;
import ru.gx.kafka.TopicMessageMode;
import ru.gx.kafka.events.OnRawDataLoadedFromIncomeTopicEvent;

import java.security.InvalidParameterException;
import java.util.*;

/**
 * Описатель обработчика одной очереди.
 */
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class AbstractIncomeTopicLoadingDescriptor implements IncomeTopicLoadingDescriptor {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Конфигурация, которой принадлежит описатель.
     */
    @Getter(AccessLevel.PROTECTED)
    @NotNull
    private final AbstractIncomeTopicsConfiguration owner;

    /**
     * Имя топика очереди.
     */
    @Getter
    @NotNull
    private final String topic;

    /**
     * Приоритет, с которым надо обрабатывать очередь.
     * 0 - высший.
     * > 0 - менее приоритетный.
     */
    @Getter
    private int priority;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Getter
    @NotNull
    private TopicMessageMode messageMode;

    /**
     * Объект-получатель сообщений.
     */
    @Getter
    private Consumer<?, ?> consumer;

    /**
     * Список смещений для каждого Partition-а.
     * Key - Partition.
     * Value - Offset.
     */
    @NotNull
    private final Map<Integer, Long> partitionOffsets = new HashMap<>();

    /**
     * Режим чтения данных из очереди (с сохранением в репозиторий, без сохранения).
     */
    @Getter
    @NotNull
    private LoadingMode loadingMode;

    @Getter
    @Nullable
    private LoadingFiltering loadingFiltering;

    /**
     * Статистика чтения и обработки данных.
     */
    @Getter
    @NotNull
    private final IncomeTopicLoadingStatistics loadingStatistics = new IncomeTopicLoadingStatistics();

    /**
     * Признак того, что описатель инициализирован.
     */
    @Getter
    private boolean initialized = false;
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">
    protected AbstractIncomeTopicLoadingDescriptor(@NotNull final AbstractIncomeTopicsConfiguration owner, @NotNull final String topic, @Nullable final IncomeTopicLoadingDescriptorsDefaults defaults) {
        this.owner = owner;
        this.topic = topic;
        this.messageMode = TopicMessageMode.OBJECT;
        this.loadingMode = LoadingMode.Auto;
        if (defaults != null) {
            this
                    .setLoadingMode(defaults.getLoadingMode())
                    .setPartitions(defaults.getPartitions())
                    .setMessageMode(defaults.getTopicMessageMode());
        }
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @param consumerProperties Свойства consumer-а, который будет создан.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue"})
    @Override
    @NotNull
    public AbstractIncomeTopicLoadingDescriptor init(@NotNull final Properties consumerProperties) throws InvalidParameterException {
        if (this.partitionOffsets.size() <= 0) {
            throw new InvalidParameterException("Not defined partitions for topic " + this.topic);
        }
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.assign(getTopicPartitions());
        this.initialized = true;
        this.owner.internalRegisterDescriptor(this);
        return this;
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "unused"})
    @Override
    @NotNull
    public AbstractIncomeTopicLoadingDescriptor init() throws InvalidParameterException {
        return this.init(this.owner.getDescriptorsDefaults().getConsumerProperties());
    }


    @SuppressWarnings("UnusedReturnValue")
    @Override
    @NotNull
    public AbstractIncomeTopicLoadingDescriptor unInit() {
        this.owner.internalUnregisterDescriptor(this);
        this.initialized = false;
        this.consumer = null;
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">
    private void checkChangeable(@NotNull final String propertyName) {
        if (isInitialized()) {
            throw new IncomeTopicsConfigurationException("Descriptor of income topic " + getTopic() + " can't change property " + propertyName + " after initialization!");
        }
    }

    public @NotNull AbstractIncomeTopicLoadingDescriptor setPriority(final int priority) {
        checkChangeable("priority");
        this.priority = priority;
        return this;
    }

    public @NotNull AbstractIncomeTopicLoadingDescriptor setMessageMode(@NotNull final TopicMessageMode messageMode) {
        checkChangeable("messageMode");
        this.messageMode = messageMode;
        return this;
    }

    public @NotNull AbstractIncomeTopicLoadingDescriptor setLoadingMode(@NotNull final LoadingMode loadingMode) {
        checkChangeable("loadingMode");
        this.loadingMode = loadingMode;
        return this;
    }

    public @NotNull AbstractIncomeTopicLoadingDescriptor setLoadingFiltering(@Nullable final LoadingFiltering loadingFiltering) {
        checkChangeable("loadingMode");
        this.loadingFiltering = loadingFiltering;
        return this;
    }

    @SuppressWarnings("unused")
    @NotNull
    public Collection<Integer> getPartitions() {
        return this.partitionOffsets.keySet();
    }

    @SuppressWarnings("unused")
    @Override
    public long getOffset(int partition) {
        return this.partitionOffsets.get(partition);
    }

    /**
     * Метод предназначен только для сохранения смещения.
     * @param partition раздел, по которому запоминаем смещение в описателе.
     * @param offset само смещение, которое запоминаем в описателе.
     */
    @NotNull
    @Override
    public AbstractIncomeTopicLoadingDescriptor setOffset(int partition, long offset) {
        this.partitionOffsets.put(partition, offset);
        return this;
    }

    /**
     * Получение коллекции TopicPartition. Формируется динамически. Изменять данную коллекцию нет смысла!
     *
     * @return Коллекция TopicPartition-ов.
     */
    @Override
    @NotNull
    public Collection<TopicPartition> getTopicPartitions() {
        final var result = new ArrayList<TopicPartition>();
        this.partitionOffsets
                .keySet()
                .forEach(p -> result.add(new TopicPartition(getTopic(), p)));
        return result;
    }

    /**
     * Из списка {@link #partitionOffsets} удаляем те, где key не в списке {@param partitions}.
     * И добавляем в {@link #partitionOffsets} новые записи с key = p, value = -1 (т.к. не знаем смещения).
     * Если в списке смещений, уже есть такой partition, то его не трогаем.
     *
     * @param partitions Список Partition-ов, который должен быть у нас.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "Convert2MethodRef"})
    @Override
    @NotNull
    public AbstractIncomeTopicLoadingDescriptor setPartitions(int... partitions) {
        // Готовим список ключей для удаления - такие PartitionOffset-ы, которых нет в списке partitions:
        final var keyForRemove = new ArrayList<Integer>();
        this.partitionOffsets.keySet().stream()
                .filter(pkey -> Arrays.stream(partitions).noneMatch(p -> p == pkey))
                .forEach(pkey -> keyForRemove.add(pkey));

        // Удаляем:
        for (var k : keyForRemove) {
            this.partitionOffsets.remove(k);
        }

        // Добавляем только, если нет:
        Arrays.stream(partitions).forEach(p -> {
            if (!this.partitionOffsets.containsKey(p)) {
                this.partitionOffsets.put(p, (long) -1);
            }
        });

        return this;
    }

    /**
     * Установка смещения для Партиции очереди.
     *
     * @param partition Партиция.
     * @param offset    Смещение.
     */
    @Override
    @NotNull
    public AbstractIncomeTopicLoadingDescriptor setProcessedPartitionOffset(int partition, long offset) {
        this.partitionOffsets.put(partition, offset);
        return this;
    }

    /**
     * Класс объектов-событий после чтения объектов (и загрузки в репозиторий).
     */
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnRawDataLoadedFromIncomeTopicEvent> onRawDataLoadedEventClass;

    @Nullable
    public OnRawDataLoadedFromIncomeTopicEvent getOnRawDataLoadedEvent(@NotNull final ApplicationContext context) {
        if (this.onRawDataLoadedEventClass != null) {
            return context.getBean(onRawDataLoadedEventClass);
        }
        return null;
    }

    /**
     * @return Строка с информацией об обработанных PartitionOffset-ах для логирования.
     */
    @Override
    @NotNull
    public String getProcessedPartitionsOffsetsInfoForLog() {
        StringBuilder result = new StringBuilder();
        for (final var p : this.partitionOffsets.keySet()) {
            if (result.length() > 0) {
                result.append("; ");
            }
            result
                    .append(p.toString())
                    .append(':')
                    .append(this.partitionOffsets.get(p));
        }
        return result.toString();
    }

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
