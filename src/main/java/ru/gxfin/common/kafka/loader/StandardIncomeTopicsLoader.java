package ru.gxfin.common.kafka.loader;

import org.springframework.context.ApplicationContext;

@SuppressWarnings("unused")
public class StandardIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    public StandardIncomeTopicsLoader(ApplicationContext context) {
        super(context);
    }
}
