package ru.gxfin.common.kafka.uploader;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;

@SuppressWarnings("unused")
public class StandardOutcomeTopicUploader extends AbstractOutcomeTopicUploader {
    public StandardOutcomeTopicUploader(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public <O extends DataObject, P extends DataPackage<O>> P createPackage(OutcomeTopicUploadingDescriptor<O, P> descriptor) throws Exception {
        return super.createPackage(descriptor);
    }
}
