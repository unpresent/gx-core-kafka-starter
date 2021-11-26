package ru.gx.core.kafka.load;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class BootstrapKafkaIncomeTopicsConfiguration extends AbstractKafkaIncomeTopicsConfiguration {
    public BootstrapKafkaIncomeTopicsConfiguration(@NotNull final String serviceName) {
        super(serviceName);
    }
}
