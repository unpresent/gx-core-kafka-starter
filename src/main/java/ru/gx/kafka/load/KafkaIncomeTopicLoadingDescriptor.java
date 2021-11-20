package ru.gx.kafka.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.channels.AbstractIncomeChannelDescriptor;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;

/**
 * Описатель обработчика одной очереди.
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class KafkaIncomeTopicLoadingDescriptor<O extends DataObject, P extends DataPackage<O>>
        extends AbstractIncomeChannelDescriptor<O, P> {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">

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
     * Время на ожидание данных при чтении.
     * Можно менять в RunTime (после инициализации).
     */
    @Getter
    @Setter
    @NotNull
    private Duration durationOnPoll;

    /**
     * Признак того, что описатель инициализирован.
     */
    @Getter
    private boolean initialized = false;
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">
    protected KafkaIncomeTopicLoadingDescriptor(@NotNull final AbstractKafkaIncomeTopicsConfiguration owner, @NotNull final String topic, @Nullable final KafkaIncomeTopicLoadingDescriptorsDefaults defaults) {
        super(owner, topic, defaults);
        this.durationOnPoll = Duration.ofMillis(100);
        if (defaults != null) {
            this
                    .setPartitions(defaults.getPartitions())
                    .setDurationOnPoll(defaults.getDurationOnPoll());
        }
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @param consumerProperties Свойства consumer-а, который будет создан.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue"})
    @NotNull
    public KafkaIncomeTopicLoadingDescriptor<O, P> init(@NotNull final Properties consumerProperties) throws InvalidParameterException {
        if (this.partitionOffsets.size() <= 0) {
            throw new InvalidParameterException("Not defined partitions for topic " + this.getName());
        }
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.assign(getTopicPartitions());
        super.init();
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
    public KafkaIncomeTopicLoadingDescriptor<O, P> init() throws InvalidParameterException {
        return this.init(this.getOwner().getDescriptorsDefaults().getConsumerProperties());
    }


    @SuppressWarnings("UnusedReturnValue")
    @Override
    @NotNull
    public KafkaIncomeTopicLoadingDescriptor<O, P> unInit() {
        this.getOwner().internalUnregisterDescriptor(this);
        this.initialized = false;
        this.consumer = null;
        return this;
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">
    @Override
    @NotNull
    public AbstractKafkaIncomeTopicsConfiguration getOwner() {
        return (AbstractKafkaIncomeTopicsConfiguration)super.getOwner();
    }

    @SuppressWarnings("unused")
    @NotNull
    public Collection<Integer> getPartitions() {
        return this.partitionOffsets.keySet();
    }

    @SuppressWarnings("unused")
    public long getOffset(int partition) {
        return this.partitionOffsets.get(partition);
    }

    /**
     * Метод предназначен только для сохранения смещения.
     * @param partition раздел, по которому запоминаем смещение в описателе.
     * @param offset само смещение, которое запоминаем в описателе.
     */
    @NotNull
    public KafkaIncomeTopicLoadingDescriptor<O, P> setOffset(int partition, long offset) {
        this.partitionOffsets.put(partition, offset);
        return this;
    }

    /**
     * Получение коллекции TopicPartition. Формируется динамически. Изменять данную коллекцию нет смысла!
     *
     * @return Коллекция TopicPartition-ов.
     */
    @NotNull
    public Collection<TopicPartition> getTopicPartitions() {
        final var result = new ArrayList<TopicPartition>();
        this.partitionOffsets
                .keySet()
                .forEach(p -> result.add(new TopicPartition(getName(), p)));
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
    @NotNull
    public KafkaIncomeTopicLoadingDescriptor<O, P> setPartitions(int... partitions) {
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
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}