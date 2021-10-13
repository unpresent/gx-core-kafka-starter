package ru.gx.kafka.upload;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.Nullable;
import ru.gx.kafka.TopicMessageMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class OutcomeTopicUploadingDescriptorsDefaults {
    /**
     * Режим представления данных в Kafka: пообъектно или пакетами.
     */
    @Getter
    @Setter
    private TopicMessageMode topicMessageMode = TopicMessageMode.OBJECT;

    /**
     * Свойства для создания Producer-а.
     */
    @Getter
    @Setter
    @Nullable
    private Properties producerProperties;

    /**
     * Максимальное количество объектов в пакете данных.
     */
    @Getter
    @Setter
    private int maxPackageSize = 100;

    private final ArrayList<Header> defaultHeaders = new ArrayList<>();

    /**
     * @return Список Header-ов, которые будут установлены у описателей.
     * @see OutcomeTopicUploadingDescriptor#getDescriptorHeaders()
     */
    public Iterable<Header> getDefaultHeaders() {
        return this.defaultHeaders;
    }

    /**
     * Полная установка списка Header-ов. Старые значения чистятся.
     * @param headers Список Header-ов, которые теперь будут у default-настройки.
     * @return this.
     */
    public OutcomeTopicUploadingDescriptorsDefaults setDefaultHeaders(Iterable<Header> headers) {
        this.defaultHeaders.clear();
        headers.forEach(this.defaultHeaders::add);
        return this;
    }

    /**
     * Добавление Header-а. Если Header с таким ключом уже есть, то заменяется.
     * @param header Новый Header, который будет записан в коллекцию Header-ов.
     * @return this.
     */
    public OutcomeTopicUploadingDescriptorsDefaults addDefaultHeader(Header header) {
        final var index = this.defaultHeaders.indexOf(header);
        if (index >= 0) {
            this.defaultHeaders.set(index, header);
        } else {
            this.defaultHeaders.add(header);
        }
        return this;
    }

    public OutcomeTopicUploadingDescriptorsDefaults setDefaultHeaders(Header... defaultHeaders) {
        this.defaultHeaders.clear();
        this.defaultHeaders.addAll(Arrays.asList(defaultHeaders));
        return this;
    }

    protected OutcomeTopicUploadingDescriptorsDefaults() {
        super();
    }
}
