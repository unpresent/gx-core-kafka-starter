package ru.gx.kafka.load;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.offsets.PartitionOffset;
import ru.gx.kafka.offsets.TopicPartitionOffset;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class StandardIncomeTopicsOffsetsController implements IncomeTopicsOffsetsController {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Смещения
     */
    @NotNull
    private final Map<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>, Map<Integer, LoadingFiltering>> offsets = new HashMap<>();

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsOffsetsController">
    @Override
    public void seekTopicsByList(@NotNull final IncomeTopicsConfiguration configuration, @NotNull final Collection<TopicPartitionOffset> offsets) {
        offsets.forEach(o -> {
            final var topicDescriptor = configuration.tryGet(o.getTopic());
            if (topicDescriptor != null) {
                internalSeekItem(topicDescriptor, o.getPartition(), o.getOffset());
            }
        });
    }

    /**
     * Требование о смещении Offset-ов на начало для всех Topic-ов и всех Partition-ов.
     */
    @Override
    public void seekAllToBegin(@NotNull final IncomeTopicsConfiguration configuration) {
        configuration.getAll().forEach(this::internalSeekTopicAllPartitionsToBegin);
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Topic-ов и всех Partition-ов.
     */
    @Override
    public void seekAllToEnd(@NotNull final IncomeTopicsConfiguration configuration) {
        configuration.getAll().forEach(this::internalSeekTopicAllPartitionsToEnd);
    }

    /**
     * Требование о смещении Offset-ов на начало для всех Partition-ов для заданного Topic-а.
     *
     * @param topicDescriptor Топик, для которого требуется сместить смещения.
     */
    @Override
    public void seekTopicAllPartitionsToBegin(@NotNull final IncomeTopicLoadingDescriptor<?, ?> topicDescriptor) {
        internalSeekTopicAllPartitionsToBegin(topicDescriptor);
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
     *
     * @param topicDescriptor Топик, для которого требуется сместить смещения.
     */
    @Override
    public void seekTopicAllPartitionsToEnd(@NotNull final IncomeTopicLoadingDescriptor<?, ?> topicDescriptor) {
        internalSeekTopicAllPartitionsToEnd(topicDescriptor);
    }

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     *
     * @param topicDescriptor  Топик, для которого требуется сместить смещения.
     * @param partitionOffsets Смещения (для каждого Partition-а свой Offset).
     */
    @Override
    public void seekTopic(@NotNull final IncomeTopicLoadingDescriptor<?, ?> topicDescriptor, @NotNull Iterable<PartitionOffset> partitionOffsets) {
        partitionOffsets
                .forEach(po -> internalSeekItem(topicDescriptor, po.getPartition(), po.getOffset()));
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя реализация">

    protected void internalSeekTopicAllPartitionsToBegin(@NotNull IncomeTopicLoadingDescriptor<?, ?> topicDescriptor) {
        this.internalSeekTopicAllPartitionsToBorder(topicDescriptor, Consumer::seekToBeginning);
    }

    protected void internalSeekTopicAllPartitionsToEnd(@NotNull IncomeTopicLoadingDescriptor<?, ?> topicDescriptor) {
        this.internalSeekTopicAllPartitionsToBorder(topicDescriptor, Consumer::seekToEnd);
    }

    protected void internalSeekTopicAllPartitionsToBorder(@NotNull IncomeTopicLoadingDescriptor<?, ?> topicDescriptor, ConsumerSeekToBorderFunction func) {
        final Collection<TopicPartition> topicPartitions = topicDescriptor.getTopicPartitions();
        final var consumer = topicDescriptor.getConsumer();
        // consumer.seekToBeginning(topicPartitions);
        func.seek(consumer, topicPartitions);
        for (var tp : topicPartitions) {
            final var position = consumer.position(tp);
            topicDescriptor.setOffset(tp.partition(), position);
        }
    }

    protected void internalSeekItem(@NotNull final IncomeTopicLoadingDescriptor<?, ?> topicDescriptor, int partition, long offset) {
        final var consumer = topicDescriptor.getConsumer();
        final var tp = new TopicPartition(topicDescriptor.getTopic(), partition);
        consumer.seek(tp, offset > 0 ? offset : 0);
        topicDescriptor.setOffset(tp.partition(), consumer.position(tp));
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------

}
