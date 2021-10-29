package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.offsets.PartitionOffset;
import ru.gx.kafka.offsets.TopicPartitionOffset;

import java.util.Collection;

@SuppressWarnings("unused")
public interface IncomeTopicsOffsetsController {

    /**
     * Требование о смещении Offset-ов на начало для всех Topic-ов и всех Partition-ов.
     */
    void seekAllToBegin(@NotNull final IncomeTopicsConfiguration configuration);

    /**
     * Требование о смещении Offset-ов на конец для всех Topic-ов и всех Partition-ов.
     */
    void seekAllToEnd(@NotNull final IncomeTopicsConfiguration configuration);

    /**
     * Требование о смещении Offset-ов на начало для всех Partition-ов для заданного Topic-а.
     *
     * @param topicDescriptor Топик, для которого требуется сместить смещения.
     */
    void seekTopicAllPartitionsToBegin(@NotNull final IncomeTopicLoadingDescriptor topicDescriptor);

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
     *
     * @param topicDescriptor Топик, для которого требуется сместить смещения.
     */
    void seekTopicAllPartitionsToEnd(@NotNull final IncomeTopicLoadingDescriptor topicDescriptor);

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     *
     * @param topicDescriptor  Топик, для которого требуется сместить смещения.
     * @param partitionOffsets Смещения (для каждого Partition-а свой Offset).
     */
    void seekTopic(@NotNull final IncomeTopicLoadingDescriptor topicDescriptor, @NotNull Iterable<PartitionOffset> partitionOffsets);

    /**
     * Требование о смещении Offset-ов на заданные значения для заданных Topic-ов и Partition-ов.
     *
     * @param configuration Конфигурация, для которой применяется команда. Если среди топиков,
     *                      указанных в {@code offsets} не будет описателя в указанной конфигурации, то такой топик игнорируется.
     * @param offsets Список троек: Топик, Партиция, Смещение.
     */
    void seekTopicsByList(@NotNull final IncomeTopicsConfiguration configuration, @NotNull final Collection<TopicPartitionOffset> offsets);

    /**
     * Получение списка смещений всех описателей конфигурации.
     * @param configuration Конфигурация, из описателей которой извлекаем смещения.
     * @return Список смещений.
     */
    Collection<TopicPartitionOffset> getOffsetsByConfiguration(@NotNull final IncomeTopicsConfiguration configuration);
}
