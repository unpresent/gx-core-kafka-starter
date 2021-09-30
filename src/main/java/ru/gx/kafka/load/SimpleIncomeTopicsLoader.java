package ru.gx.kafka.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public class SimpleIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    public SimpleIncomeTopicsLoader(@NotNull ObjectMapper objectMapper) {
        super(objectMapper);
    }
}
