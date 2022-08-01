package ru.gx.core.kafka.load;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.ChannelHandlerDescriptor;
import ru.gx.core.kafka.offsets.PartitionOffset;
import ru.gx.core.kafka.offsets.TopicPartitionOffset;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;

import java.util.ArrayList;
import java.util.Collection;

@SuppressWarnings("unused")
public class KafkaIncomeTopicsOffsetsController {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsOffsetsController">

    /**
     * Требование о смещении Offset-ов на заданные значения для заданных Topic-ов и Partition-ов.
     *
     * @param configuration Конфигурация, для которой применяется команда. Если среди топиков,
     *                      указанных в {@code offsets} не будет описателя в указанной конфигурации, то такой топик игнорируется.
     * @param offsets       Список троек: Топик, Партиция, Смещение.
     */
    public void seekTopicsByList(
            @NotNull final AbstractKafkaIncomeTopicsConfiguration configuration,
            @NotNull final Collection<TopicPartitionOffset> offsets
    ) {
        final var list = new ArrayList<ChannelHandlerDescriptor>();
        configuration.getAll().forEach(list::add);

        offsets.forEach(o -> {
            final var topicDescriptor =
                    configuration.tryGet(o.getTopic());
            if (topicDescriptor instanceof
                    final KafkaIncomeTopicLoadingDescriptor kafkaDescriptor) {
                internalSeekItem(kafkaDescriptor, o.getPartition(), o.getOffset());
                list.remove(kafkaDescriptor);
            }
        });

        list.forEach(this::internalSeekTopicAllPartitionsToBegin);
    }

    /**
     * Требование о смещении Offset-ов на начало для всех Topic-ов и всех Partition-ов.
     */
    public void seekAllToBegin(@NotNull final AbstractKafkaIncomeTopicsConfiguration configuration) {
        final var channels = configuration.getAll();
        channels.forEach(this::internalSeekTopicAllPartitionsToBegin);
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Topic-ов и всех Partition-ов.
     */
    public void seekAllToEnd(@NotNull final AbstractKafkaIncomeTopicsConfiguration configuration) {
        configuration.getAll().forEach(this::internalSeekTopicAllPartitionsToEnd);
    }

    /**
     * Требование о смещении Offset-ов на начало для всех Partition-ов для заданного Topic-а.
     *
     * @param topicDescriptor Топик, для которого требуется сместить смещения.
     */
    public <M extends Message<? extends MessageBody>>
    void seekTopicAllPartitionsToBegin(
            @NotNull final KafkaIncomeTopicLoadingDescriptor topicDescriptor
    ) {
        internalSeekTopicAllPartitionsToBegin(topicDescriptor);
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
     *
     * @param topicDescriptor Топик, для которого требуется сместить смещения.
     */
    public <M extends Message<? extends MessageBody>>
    void seekTopicAllPartitionsToEnd(
            @NotNull final KafkaIncomeTopicLoadingDescriptor topicDescriptor
    ) {
        internalSeekTopicAllPartitionsToEnd(topicDescriptor);
    }

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     *
     * @param topicDescriptor  Топик, для которого требуется сместить смещения.
     * @param partitionOffsets Смещения (для каждого Partition-а свой Offset).
     */
    public <M extends Message<? extends MessageBody>>
    void seekTopic(
            @NotNull final KafkaIncomeTopicLoadingDescriptor topicDescriptor,
            @NotNull final Iterable<PartitionOffset> partitionOffsets
    ) {
        partitionOffsets
                .forEach(po -> internalSeekItem(topicDescriptor, po.getPartition(), po.getOffset()));
    }

    /**
     * Получение списка смещений всех описателей конфигурации.
     *
     * @param configuration Конфигурация, из описателей которой извлекаем смещения.
     * @return Список смещений.
     */
    public Collection<TopicPartitionOffset> getOffsetsByConfiguration(
            @NotNull AbstractKafkaIncomeTopicsConfiguration configuration
    ) {
        final var offsets = new ArrayList<TopicPartitionOffset>();
        configuration.getAll()
                .forEach(channelDescriptor -> {
                    final var topicDescriptor = (KafkaIncomeTopicLoadingDescriptor) channelDescriptor;
                    topicDescriptor.getPartitions()
                            .forEach(p -> offsets
                                    .add(new TopicPartitionOffset(
                                            topicDescriptor.getChannelName(),
                                            p,
                                            topicDescriptor.getOffset(p))
                                    ));
                });
        return offsets;
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя реализация">
    protected <M extends Message<? extends MessageBody>>
    void internalSeekTopicAllPartitionsToBegin(@NotNull ChannelHandlerDescriptor topicDescriptor) {
        this.internalSeekTopicAllPartitionsToBorder(
                (KafkaIncomeTopicLoadingDescriptor) topicDescriptor,
                Consumer::seekToBeginning
        );
    }

    protected <M extends Message<? extends MessageBody>>
    void internalSeekTopicAllPartitionsToEnd(@NotNull ChannelHandlerDescriptor topicDescriptor) {
        this.internalSeekTopicAllPartitionsToBorder(
                (KafkaIncomeTopicLoadingDescriptor) topicDescriptor,
                Consumer::seekToEnd
        );
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected <M extends Message<? extends MessageBody>>
    void internalSeekTopicAllPartitionsToBorder(
            @NotNull final KafkaIncomeTopicLoadingDescriptor topicDescriptor,
            @NotNull final ConsumerSeekToBorderFunction func
    ) {
        final Collection<TopicPartition> topicPartitions = topicDescriptor.getTopicPartitions();
        final var consumer = topicDescriptor.getConsumer();
        synchronized (consumer) {
            func.seek(consumer, topicPartitions);
            for (var tp : topicPartitions) {
                final var position = consumer.position(tp);
                topicDescriptor.setOffset(tp.partition(), position);
            }
        }
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected <M extends Message<? extends MessageBody>>
    void internalSeekItem(
            @NotNull final KafkaIncomeTopicLoadingDescriptor channelDescriptor,
            int partition,
            long offset
    ) {
        final var tp = new TopicPartition(channelDescriptor.getChannelName(), partition);
        final var consumer = channelDescriptor.getConsumer();
        synchronized (consumer) {
            consumer.seek(tp, offset > 0 ? offset : 0);
            channelDescriptor.setOffset(tp.partition(), consumer.position(tp));
        }
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    @FunctionalInterface
    protected interface ConsumerSeekToBorderFunction {
        void seek(
                @NotNull final Consumer<?, ?> consumer,
                @NotNull final Collection<TopicPartition> topicPartitions
        );
    }
}
