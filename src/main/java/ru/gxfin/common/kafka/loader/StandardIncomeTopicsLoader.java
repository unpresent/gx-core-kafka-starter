package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationContext;

@SuppressWarnings("unused")
public class StandardIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    public StandardIncomeTopicsLoader(ApplicationContext context, ObjectMapper objectMapper) {
        super(context, objectMapper);
    }
}
