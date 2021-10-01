package ru.gx.kafka.upload;

import org.jetbrains.annotations.NotNull;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

@SuppressWarnings("unused")
public class SimpleOutcomeTopicUploader extends AbstractOutcomeTopicUploader {
    public SimpleOutcomeTopicUploader() {
        super();
    }

    @Override
    @NotNull
    public <O extends DataObject, P extends DataPackage<O>> P createPackage(@NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor) throws Exception {
        return super.createPackage(descriptor);
    }
}
