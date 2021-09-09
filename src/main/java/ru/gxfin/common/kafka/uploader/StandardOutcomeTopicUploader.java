package ru.gxfin.common.kafka.uploader;

import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("unused")
public class StandardOutcomeTopicUploader extends AbstractOutcomeTopicUploader {
    public StandardOutcomeTopicUploader(ObjectMapper objectMapper) {
        super(objectMapper);
    }
}
