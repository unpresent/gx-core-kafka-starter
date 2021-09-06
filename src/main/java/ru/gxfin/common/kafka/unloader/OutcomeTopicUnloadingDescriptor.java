package ru.gxfin.common.kafka.unloader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.Producer;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.TopicMessageMode;

import java.lang.reflect.ParameterizedType;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class OutcomeTopicUnloadingDescriptor<O extends DataObject, P extends DataPackage<O>> {
    /**
     * Имя топика очереди.
     */
    @Getter
    private final String topic;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Getter
    @Setter
    private TopicMessageMode messageMode;

    @Getter
    @Setter
    private Class<? extends O> dataObjectClass;

    @Getter
    @Setter
    private Class<? extends P> dataPackageClass;

    @Getter
    @Setter
    private Producer<Long, String> producer;

    @SuppressWarnings("unchecked")
    public OutcomeTopicUnloadingDescriptor(String topic) {
        this.topic = topic;

        final var thisClass = this.getClass();
        final var superClass = thisClass.getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            this.dataObjectClass = (Class<O>) ((ParameterizedType) superClass).getActualTypeArguments()[0];
            this.dataPackageClass = (Class<P>) ((ParameterizedType) superClass).getActualTypeArguments()[1];
        }
    }
}
