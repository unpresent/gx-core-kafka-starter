package ru.gx.kafka.upload;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.SerializeMode;
import ru.gx.kafka.TopicMessageMode;
import ru.gx.kafka.load.IncomeTopicLoadingDescriptor;

import java.security.InvalidParameterException;
import java.util.Properties;

public interface OutcomeTopicUploadingDescriptor {
    /**
     * Имя топика очереди.
     */
    @NotNull
    String getTopic();

    /**
     * Приоритет, с которым надо обрабатывать очередь.
     * 0 - высший.
     * > 0 - менее приоритетный.
     */
    int getPriority();

    /**
     * Установка приоритета у топика.
     * @param priority приоритет.
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor setPriority(int priority);

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @NotNull
    TopicMessageMode getMessageMode();

    /**
     * @param messageMode Режим данных в очереди: Пообъектно и пакетно.
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor setMessageMode(@NotNull final TopicMessageMode messageMode);

    /**
     * Режим сериализации: Строки или Байты.
     */
    @NotNull
    SerializeMode getSerializeMode();

    /**
     * @param serializeMode Режим сериализации: Строки или Байты.
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor setSerializeMode(@NotNull final SerializeMode serializeMode);

    /**
     * Producer - публикатор сообщений в Kafka
     */
    Producer<Long, ?> getProducer();

    /**
     * @return Список Header-ов для отправляемых сообщений.
     */
    @NotNull
    Iterable<Header> getDescriptorHeaders();

    /**
     * @return Количество Header-ов для отправляемых сообщений.
     */
    int getDescriptorHeadersSize();

    /**
     * Заменить список всех Header-ов на целевой набор.
     * @param headers целевой список Header-ов.
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor setDescriptorHeaders(@NotNull final Iterable<Header> headers);

    /**
     * Добавление одного Header-а (если header с таким ключом уже есть, то замена).
     * @param header добавляемый Header.
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor addDescriptorHeader(Header header);

    /**
     * Добавление списка Header-а (если header-ы с такими ключами уже есть, то замена).
     * @param headers добавляемые Header-ы.
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor addDescriptorHeaders(@NotNull final Iterable<Header> headers);

    /**
     * Признак того, что описатель инициализирован
     */
    boolean isInitialized();

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @param producerProperties Свойства Producer-а, который будет создан.
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor init(@NotNull final Properties producerProperties) throws InvalidParameterException;

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @NotNull
    OutcomeTopicUploadingDescriptor init() throws InvalidParameterException;

    OutcomeTopicUploadingDescriptor unInit();
}
