package ru.gx.kafka.uploader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

@SuppressWarnings("unused")
public class StandardOutcomeTopicUploader extends AbstractOutcomeTopicUploader {
    public StandardOutcomeTopicUploader(@NotNull ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>> P createPackage(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor) throws Exception {
        return super.createPackage(descriptor);
    }
}
