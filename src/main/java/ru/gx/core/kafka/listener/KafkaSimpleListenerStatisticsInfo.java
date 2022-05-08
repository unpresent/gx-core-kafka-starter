package ru.gx.core.kafka.listener;

import io.micrometer.core.instrument.MeterRegistry;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.worker.AbstractWorker;
import ru.gx.core.worker.AbstractWorkerStatisticsInfo;

public class KafkaSimpleListenerStatisticsInfo extends AbstractWorkerStatisticsInfo {
    public KafkaSimpleListenerStatisticsInfo(@NotNull AbstractWorker worker, @NotNull MeterRegistry meterRegistry) {
        super(worker, meterRegistry);
    }
}
