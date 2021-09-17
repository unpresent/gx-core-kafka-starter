package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationContext;

@SuppressWarnings("unused")
public class StandardIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    public StandardIncomeTopicsLoader(@NotNull ApplicationContext context, @NotNull ObjectMapper objectMapper) {
        super(context, objectMapper);
    }
}
