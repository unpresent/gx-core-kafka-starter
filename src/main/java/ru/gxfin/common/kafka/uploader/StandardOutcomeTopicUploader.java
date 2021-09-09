package ru.gxfin.common.kafka.uploader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationContext;

@SuppressWarnings("unused")
public class StandardOutcomeTopicUploader extends AbstractOutcomeTopicUploader {
    protected StandardOutcomeTopicUploader(ApplicationContext context, ObjectMapper objectMapper) {
        super(context, objectMapper);
    }
}
