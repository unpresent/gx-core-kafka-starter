package ru.gx.core.kafka.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.gx.core.kafka.load.AbstractKafkaIncomeTopicsConfiguration;
import ru.gx.core.kafka.load.KafkaIncomeTopicLoadingDescriptor;
import ru.gx.core.kafka.load.KafkaIncomeTopicsOffsetsController;
import ru.gx.core.kafka.offsets.PartitionOffset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

@ConditionalOnProperty(name = "service.kafka.offsets-rest-controllers.income.enabled", havingValue = "true")
@ConditionalOnWebApplication
@RestController
@RequiredArgsConstructor
@Tag(name = "Управление входящими смещениями kafka.",
        description = "Позволяет получать и устанавливать входящие смещения kafka.")
public class IncomeOffsetsRestController {

    @NotNull
    final Collection<AbstractKafkaIncomeTopicsConfiguration> configurations;

    @NotNull
    final KafkaIncomeTopicsOffsetsController offsetsController;

    @SuppressWarnings("unused")
    @GetMapping("/kafka/get-income-offsets")
    @Operation(summary = "Получить входящие смещения по имени топика kafka.")
    public Collection<PartitionOffset> getOffsets(
            @Parameter(description = "Имя топика kafka.")
            @RequestParam("topic") @NotNull final String topic
    ) {
        final var result = new ArrayList<PartitionOffset>();
        this.configurations.forEach(config -> {
            if (config.contains(topic)) {
                final var descriptor = (KafkaIncomeTopicLoadingDescriptor) config.get(topic);
                descriptor.getPartitions()
                        .forEach(p -> result.add(new PartitionOffset(p, descriptor.getOffset(p))));
            }
        });
        return result;
    }

    @SuppressWarnings("unused")
    @PostMapping("/kafka/change-income-offset")
    @Operation(summary = "Изменить входящие смещения kafka.")
    public boolean changeIncomeOffset(
            @Parameter(description = "Имя входящего топика kafka.")
            @RequestParam("topic") @NotNull final String topic,
            @Parameter(description = "Номер раздела.")
            @RequestParam("partition") final int partition,
            @Parameter(description = "Значение для установки смещения.")
            @RequestParam("offset") final long offset
    ) {
        final var result = new AtomicBoolean(false);
        this.configurations.forEach(config -> {
            if (config.contains(topic)) {
                final var descriptor = (KafkaIncomeTopicLoadingDescriptor) config.get(topic);
                final var partitionOffsets = new ArrayList<PartitionOffset>();
                partitionOffsets.add(new PartitionOffset(partition, offset));
                this.offsetsController.seekTopic(descriptor, partitionOffsets);
                result.set(true);
            }
        });
        return result.get();
    }
}
