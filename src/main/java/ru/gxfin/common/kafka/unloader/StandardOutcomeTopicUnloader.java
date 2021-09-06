package ru.gxfin.common.kafka.unloader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationContext;

@SuppressWarnings("unused")
public class StandardOutcomeTopicUnloader extends AbstractOutcomeTopicUnloader {
    protected StandardOutcomeTopicUnloader(ApplicationContext context, ObjectMapper objectMapper) {
        super(context, objectMapper);
    }
}
