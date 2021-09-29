package ru.gx.kafka.uploader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.Producer;
import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.TopicMessageMode;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

import java.lang.reflect.ParameterizedType;

@Getter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class OutcomeTopicUploadingDescriptor<O extends DataObject, P extends DataPackage<O>> {
    /**
     * Имя топика очереди.
     */
    private final String topic;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Setter
    private TopicMessageMode messageMode;

    @Setter
    private Class<? extends O> dataObjectClass;

    @Setter
    private Class<? extends P> dataPackageClass;

    @Setter
    private Producer<Long, String> producer;

    @SuppressWarnings("unchecked")
    public OutcomeTopicUploadingDescriptor(@NotNull String topic, OutcomeTopicUploadingDescriptorsDefaults defaults) {
        this.topic = topic;

        final var thisClass = this.getClass();
        final var superClass = thisClass.getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            this.dataObjectClass = (Class<O>) ((ParameterizedType) superClass).getActualTypeArguments()[0];
            this.dataPackageClass = (Class<P>) ((ParameterizedType) superClass).getActualTypeArguments()[1];
        }

        if (defaults != null) {
            this
                    .setProducer(defaults.getProducer())
                    .setMessageMode(defaults.getTopicMessageMode());
        }
    }
}
