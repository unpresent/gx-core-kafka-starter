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
public class OutcomeTopicUploadingDescriptor<O extends DataObject, P extends DataPackage<O>> {
    /**
     * Имя топика очереди.
     */
    @Getter
    @NotNull
    private final String topic;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Getter
    @Setter
    private TopicMessageMode messageMode;

    @Getter
    @Setter
    @Nullable
    private DataMemoryRepository<O, P> memoryRepository;

    /**
     * Класс объектов данных
     */
    @Getter
    @Setter
    private Class<? extends O> dataObjectClass;

    /**
     * Класс пакетов объектов данных
     */
    @Getter
    @Setter
    private Class<? extends P> dataPackageClass;

    @Getter
    @Setter
    private int maxPackageSize = 100;

    /**
     * Producer - публикатор сообщений в Kafka
     */
    @Getter
    private Producer<Long, String> producer;

    @NotNull
    private final ArrayList<Header> descriptorHeaders = new ArrayList<>();

    public int getDescriptorHeadersSize() {
        return this.descriptorHeaders.size();
    }

    @NotNull
    public Iterable<Header> getDescriptorHeaders() {
        return this.descriptorHeaders;
    }

    @SuppressWarnings("UnusedReturnValue")
    @NotNull
    public final OutcomeTopicUploadingDescriptor<O, P> setDescriptorHeaders(Iterable<Header> headers) {
        this.descriptorHeaders.clear();
        headers.forEach(this.descriptorHeaders::add);
        return this;
    }

    @SuppressWarnings("unused")
    @NotNull
    public final OutcomeTopicUploadingDescriptor<O, P> addDescriptorHeader(Header header) {
        final var index = this.descriptorHeaders.indexOf(header);
        if (index >= 0) {
            this.descriptorHeaders.set(index, header);
        } else {
            this.descriptorHeaders.add(header);
        }
        return this;
    }

    /**
     * Признак того, что описатель инициализирован
     */
    @Getter
    private boolean initialized;

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
                    .setMessageMode(defaults.getTopicMessageMode())
                    .setDescriptorHeaders(defaults.getDefaultHeaders())
                    .setMaxPackageSize(defaults.getMaxPackageSize());
        }
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "unused"})
    @NotNull
    public OutcomeTopicUploadingDescriptor<O, P> init(@NotNull final Properties producerProperties) throws InvalidParameterException {
        if (this.dataObjectClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[0].");
        }
        if (this.dataPackageClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[1].");
        }

        this.producer = new KafkaProducer<>(producerProperties);
        this.initialized = true;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public OutcomeTopicUploadingDescriptor<O, P> unInit() {
        this.initialized = false;
        this.producer = null;
        return this;
    }
}
