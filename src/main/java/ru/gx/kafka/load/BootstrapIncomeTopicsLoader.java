package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class BootstrapIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    public BootstrapIncomeTopicsLoader(@NotNull final String readerName) {
        super(readerName);
    }
}
