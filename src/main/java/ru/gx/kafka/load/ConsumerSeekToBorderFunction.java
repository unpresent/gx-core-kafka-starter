package ru.gx.kafka.load;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

@FunctionalInterface
public interface ConsumerSeekToBorderFunction {
    void seek(@NotNull final Consumer<?, ?> consumer, @NotNull final Collection<TopicPartition> topicPartitions);
}
