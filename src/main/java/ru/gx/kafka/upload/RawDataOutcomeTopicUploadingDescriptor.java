package ru.gx.kafka.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataMemoryRepository;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.lang.reflect.ParameterizedType;
import java.security.InvalidParameterException;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class RawDataOutcomeTopicUploadingDescriptor
    extends AbstractOutcomeTopicUploadingDescriptor {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">
    public RawDataOutcomeTopicUploadingDescriptor(@NotNull final AbstractOutcomeTopicsConfiguration owner, @NotNull final String topic, final OutcomeTopicUploadingDescriptorsDefaults defaults) {
        super(owner, topic, defaults);
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
