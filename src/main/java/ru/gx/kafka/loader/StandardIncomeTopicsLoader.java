package ru.gx.kafka.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class StandardIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    public StandardIncomeTopicsLoader(@NotNull ObjectMapper objectMapper) {
        super(objectMapper);
    }
}
