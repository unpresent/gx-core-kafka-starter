package ru.gx.core.kafka.controller;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.gx.core.kafka.load.AbstractKafkaIncomeTopicsConfiguration;
import ru.gx.core.kafka.load.KafkaIncomeTopicLoadingDescriptor;
import ru.gx.core.kafka.load.KafkaIncomeTopicsOffsetsController;
import ru.gx.core.kafka.offsets.PartitionOffset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("ClassCanBeRecord")
@ConditionalOnProperty(name = "service.kafka.offsets-controllers.income.enabled", havingValue = "true")
@RestController
@RequestMapping("/kafka-offsets")
@RequiredArgsConstructor
public class IncomeOffsetsRestController {

    @NotNull
    final Collection<AbstractKafkaIncomeTopicsConfiguration> configurations;

    @NotNull
    final KafkaIncomeTopicsOffsetsController offsetsController;

    @SuppressWarnings("unused")
    @GetMapping("/get-income-offsets")
    public Collection<PartitionOffset> getOffsets(
            @RequestParam("topic") @NotNull final String topic
    ) {
        final var result = new ArrayList<PartitionOffset>();
        this.configurations.forEach(config -> {
            if (config.contains(topic)) {
                final var descriptor = (KafkaIncomeTopicLoadingDescriptor)config.get(topic);
                descriptor.getPartitions()
                        .forEach(p -> result.add(new PartitionOffset(p, descriptor.getOffset(p))));
            }
        });
        return result;
    }

    @SuppressWarnings("unused")
    @GetMapping("/change-income-offset")
    public boolean changeIncomeOffset(
            @RequestParam("topic") @NotNull final String topic,
            @RequestParam("partition") final int partition,
            @RequestParam("offset") final long offset
    ) {
        final var result = new AtomicBoolean(false);
        this.configurations.forEach(config -> {
            if (config.contains(topic)) {
                final var descriptor = (KafkaIncomeTopicLoadingDescriptor)config.get(topic);
                final var partitionOffsets = new ArrayList<PartitionOffset>();
                partitionOffsets.add(new PartitionOffset(partition, offset));
                this.offsetsController.seekTopic(descriptor, partitionOffsets);
                result.set(true);
            }
        });
        return result.get();
    }
}
