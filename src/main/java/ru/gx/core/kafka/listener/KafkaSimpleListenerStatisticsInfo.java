package ru.gx.core.kafka.listener;

import io.micrometer.core.instrument.MeterRegistry;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.kafka.load.KafkaIncomeTopicLoadingDescriptor;
import ru.gx.core.worker.AbstractWorker;
import ru.gx.core.worker.AbstractWorkerStatisticsInfo;

import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaSimpleListenerStatisticsInfo extends AbstractWorkerStatisticsInfo {
    public KafkaSimpleListenerStatisticsInfo(@NotNull AbstractWorker worker, @NotNull MeterRegistry meterRegistry) {
        super(worker, meterRegistry);
    }

    @Override
    public String getPrintableInfo() {
        final var offsetsInfo = new StringBuilder();
        final var listener = (KafkaSimpleListener) super.getOwner();
        var isFirstStat = new AtomicBoolean(true);
        listener.getKafkaIncomeTopicsConfigurations()
                .forEach(
                        configuration -> configuration.getAll()
                                .forEach(
                                        descriptor -> {
                                            if (isFirstStat.get()) {
                                                isFirstStat.set(false);
                                            } else {
                                                offsetsInfo.append("; ");
                                            }
                                            if (descriptor.isBlockedByError()) {
                                                offsetsInfo.append("<ERR>");
                                            }
                                            offsetsInfo.append(descriptor.getChannelName());
                                            offsetsInfo.append(":");
                                            final var kafkaDescriptor =
                                                    (KafkaIncomeTopicLoadingDescriptor) descriptor;
                                            kafkaDescriptor.getPartitions()
                                                    .forEach(
                                                            p -> offsetsInfo.append(kafkaDescriptor.getOffset(p))
                                                    );
                                        }
                                )
                );
        return super.getPrintableInfo()
                + "; offsets [" + offsetsInfo + "]";
    }
}
