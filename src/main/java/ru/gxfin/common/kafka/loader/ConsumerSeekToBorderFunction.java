package ru.gxfin.common.kafka.loader;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

@FunctionalInterface
public interface ConsumerSeekToBorderFunction {
    void seek(@NotNull Consumer<?, ?> consumer, Collection<TopicPartition> topicPartitions);
}
