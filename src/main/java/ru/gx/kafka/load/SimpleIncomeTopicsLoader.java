package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class SimpleIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    public SimpleIncomeTopicsLoader(@NotNull final String readerName) {
        super(readerName);
    }
}
