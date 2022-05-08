package ru.gx.core.kafka.load;

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
import ru.gx.core.channels.AbstractIncomeChannelHandlerDescriptor;
import ru.gx.core.channels.ChannelApiDescriptor;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessageHeader;

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
public class KafkaIncomeTopicLoadingDescriptor<M extends Message<? extends MessageBody>>
        extends AbstractIncomeChannelHandlerDescriptor<M> {
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

    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">
    public KafkaIncomeTopicLoadingDescriptor(
            @NotNull final AbstractKafkaIncomeTopicsConfiguration owner,
            @NotNull final ChannelApiDescriptor<M> api,
            @Nullable final KafkaIncomeTopicLoadingDescriptorsDefaults defaults
    ) {
        super(owner, api, defaults);
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
    public KafkaIncomeTopicLoadingDescriptor<M> init(
            @NotNull final Properties consumerProperties
    ) throws InvalidParameterException {
        if (this.partitionOffsets.size() <= 0) {
            throw new InvalidParameterException("Not defined partitions for topic " + this.getApi().getName());
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
    public KafkaIncomeTopicLoadingDescriptor<M> init() throws InvalidParameterException {
        return this.init(this.getOwner().getDescriptorsDefaults().getConsumerProperties());
    }


    @SuppressWarnings("UnusedReturnValue")
    @Override
    @NotNull
    public KafkaIncomeTopicLoadingDescriptor<M> unInit() {
        this.getOwner().internalUnregisterDescriptor(this);
        this.consumer = null;
        super.unInit();
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
    public KafkaIncomeTopicLoadingDescriptor<M> setOffset(int partition, long offset) {
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
                .forEach(p -> result.add(new TopicPartition(this.getApi().getName(), p)));
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
    public KafkaIncomeTopicLoadingDescriptor<M> setPartitions(int... partitions) {
        checkMutable("partitions");
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
