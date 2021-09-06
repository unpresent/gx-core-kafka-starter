package ru.gxfin.common.kafka.loader;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@FunctionalInterface
public interface ConsumerSeekToBorderFunction {
    void seek(Consumer<?, ?> consumer, Collection<TopicPartition> topicPartitions);
}
