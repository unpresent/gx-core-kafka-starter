package ru.gx.kafka.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.data.DataMemoryRepository;
import ru.gx.kafka.TopicMessageMode;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.lang.reflect.ParameterizedType;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class StandardOutcomeTopicUploadingDescriptor<O extends DataObject, P extends DataPackage<O>>
    extends AbstractOutcomeTopicUploadingDescriptor {
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    @Getter
    @Setter
    @Nullable
    private DataMemoryRepository<O, P> memoryRepository;

    /**
     * Класс объектов данных
     */
    @Getter
    @Setter
    private Class<O> dataObjectClass;

    /**
     * Класс пакетов объектов данных
     */
    @Getter
    @Setter
    private Class<P> dataPackageClass;

    /**
     * Ограничение по количеству Объектов в пакете (если отправка данных пакетная).
     */
    @Getter
    @Setter
    private int maxPackageSize;
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialize">
    @SuppressWarnings("unchecked")
    public StandardOutcomeTopicUploadingDescriptor(@NotNull final AbstractOutcomeTopicsConfiguration owner, @NotNull final String topic, final OutcomeTopicUploadingDescriptorsDefaults defaults) {
        super(owner, topic, defaults);

        final var thisClass = this.getClass();
        final var superClass = thisClass.getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            this.dataObjectClass = (Class<O>) ((ParameterizedType) superClass).getActualTypeArguments()[0];
            this.dataPackageClass = (Class<P>) ((ParameterizedType) superClass).getActualTypeArguments()[1];
        }
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Additional getters & setters">

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @SuppressWarnings("unchecked")
    @Override
    @NotNull
    public StandardOutcomeTopicUploadingDescriptor<O, P> init(@NotNull final Properties producerProperties) throws InvalidParameterException {
        if (this.dataObjectClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[0].");
        }
        if (this.dataPackageClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[1].");
        }
        return (StandardOutcomeTopicUploadingDescriptor<O, P>) super.init(producerProperties);
    }
    // </editor-fold>
    // -----------------------------------------------------------------------------------------------------------------
}
