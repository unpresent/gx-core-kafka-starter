package ru.gx.core.kafka.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationEventPublisher;
import ru.gx.core.channels.ChannelConfigurationException;
import ru.gx.core.channels.IncomeDataProcessType;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.kafka.KafkaConstants;
import ru.gx.core.messaging.Message;
import ru.gx.core.messaging.MessageBody;
import ru.gx.core.messaging.MessagesPrioritizedQueue;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@SuppressWarnings({"unused"})
@Slf4j
public class KafkaIncomeTopicsLoader {
    private final static int MAX_SLEEP_MS = 64;

    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Требуется для отправки сообщений в обработку.
     */
    @Getter(PROTECTED)
    @NotNull
    private final MessagesPrioritizedQueue messagesQueue;

    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter(PROTECTED)
    @NotNull
    private final ObjectMapper objectMapper;

    /**
     * Требуется для отправки сообщений в обработку.
     */
    @Getter(PROTECTED)
    @NotNull
    private final ApplicationEventPublisher eventPublisher;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public KafkaIncomeTopicsLoader(
            @NotNull final ApplicationEventPublisher eventPublisher,
            @NotNull final ObjectMapper objectMapper,
            @NotNull final MessagesPrioritizedQueue messagesQueue
    ) {
        this.objectMapper = objectMapper;
        this.messagesQueue = messagesQueue;
        this.eventPublisher = eventPublisher;
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsLoader">

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     *
     * @param descriptor Описатель загрузки из Топика.
     * @return Количество загруженных объектов. -1 при наличии блокирующей ошибке в канале.
     */
    public <B extends MessageBody, M extends Message<B>>
    int processByTopic(@NotNull final KafkaIncomeTopicLoadingDescriptor descriptor) {
        checkDescriptorIsActive(descriptor);
        if (descriptor.isBlockedByError()) {
            return -1;
        }
        return internalProcessDescriptor(descriptor);
    }

    /**
     * Чтение объектов из очередей в порядке определенной в конфигурации.
     *
     * @return Map-а, в которой для каждого дескриптора указан список загруженных объектов.
     */
    @NotNull
    public Map<KafkaIncomeTopicLoadingDescriptor, Integer>
    processAllTopics(@NotNull final AbstractKafkaIncomeTopicsConfiguration configuration) throws InvalidParameterException {
        final var pCount = configuration.prioritiesCount();
        final var result = new HashMap<KafkaIncomeTopicLoadingDescriptor, Integer>();
        for (int p = 0; p < pCount; p++) {
            final var topicDescriptors = configuration.getByPriority(p);
            if (topicDescriptors == null) {
                throw new ChannelConfigurationException("Invalid null value getByPriority(" + p + ")");
            }
            for (var topicDescriptor : topicDescriptors) {
                if (topicDescriptor.isEnabled()) {
                    final var kafkaDescriptor = (KafkaIncomeTopicLoadingDescriptor) topicDescriptor;
                    log.debug("Loading working data from topic: {}", topicDescriptor.getChannelName());
                    final var eventsCount = this.processByTopic(kafkaDescriptor);
                    result.put(kafkaDescriptor, eventsCount);
                    log.debug("Loaded working data from topic. Events: {}", kafkaDescriptor.getChannelName());
                }
            }
        }
        return result;
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя реализация">

    /**
     * Проверка описателя на то, что прошла инициализация. Работать с неинициализированным описателем нельзя.
     *
     * @param descriptor описатель, который проверяем.
     */
    protected void checkDescriptorIsActive(@NotNull final KafkaIncomeTopicLoadingDescriptor descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Channel descriptor " + descriptor.getChannelName() + " is not initialized!");
        }
        if (!descriptor.isEnabled()) {
            throw new ChannelConfigurationException("Channel descriptor " + descriptor.getChannelName() + " is not enabled!");
        }
    }

    /**
     * Обработка входящих данных для указанного канала.
     *
     * @param descriptor Описатель загрузки из Топика.
     * @return Количество обработанных сообщений.
     */
    @SneakyThrows
    protected <B extends MessageBody, M extends Message<B>>
    int internalProcessDescriptor(@NotNull final KafkaIncomeTopicLoadingDescriptor descriptor) {
        // TODO: Добавить сбор статистики
        final var records = internalPoll(descriptor);
        var messagesCount = 0; // Количество сообщений. Для статистики.

        for (var rec : records) {
            internalProcessRecord(descriptor, rec);
            messagesCount++;
        }
        return messagesCount;
    }

    /**
     * Данный метод создает объект-событие, сохраняя в него данные и метаданные. Метаданные копируются из Header-ов сообщения,
     * а также добавляется смещение из Kafka, с которым была полученная запись ({@code record}).<br/>
     * Если в описателе канала {@code descriptor} указано, что обработка должна быть немедленной ({@link KafkaIncomeTopicLoadingDescriptor#getProcessType()}),
     * то событие бросается непосредственно в этом потоке.<br/>
     * Иначе пытаемся бросить событие в {@code eventsQueue}.
     * Перед вызовом {@link MessagesPrioritizedQueue#pushMessage(int, Object)} сначала проверяем, можно ли в очередь положить событие:
     * {@link MessagesPrioritizedQueue#allowPush()}
     *
     * @param descriptor Описатель канала.
     * @param record     Запись, полученная из Kafka.
     */
    @SuppressWarnings({"BusyWait", "unchecked"})
    @SneakyThrows({InterruptedException.class, IOException.class})
    protected <B extends MessageBody, M extends Message<B>>
    // <M extends Message<? extends MessageBody>> // Fuck! Так не признает!
    void internalProcessRecord(
            @NotNull final KafkaIncomeTopicLoadingDescriptor descriptor,
            @NotNull final ConsumerRecord<Object, Object> record
    ) {
        // Формируем объект-событие.
        M message;
        final var api = descriptor.getApi();
        if (api == null) {
            throw new NullPointerException("descriptor.getApi() is null!");
        }
        if (api.getSerializeMode() == SerializeMode.JsonString) {
            final var strValue = (String) record.value();
            message = (M)this.objectMapper.readValue(strValue, api.getMessageClass());
        } else {
            final var strValue = (byte[]) record.value();
            message = (M)this.objectMapper.readValue(strValue, api.getMessageClass());
        }
        message.setChannelDescriptor(descriptor);

        // Копируем все Header-ы в Metadata.
        final var headers = record.headers();
        if (headers != null) {
            headers.forEach(h -> message.putMetadata(h.key(), h.value()));
        }

        // Чтобы при обработке бизнес данных можно было сохранить смещение, которое успешно обработано.
        message.putMetadata(KafkaConstants.METADATA_PARTITION, record.partition());
        message.putMetadata(KafkaConstants.METADATA_OFFSET, record.offset());

        if (descriptor.getLoadingFiltrator() != null && !descriptor.getLoadingFiltrator().allowProcess(message)) {
            // Данные не пропущены фильтром
            internalSkippedMessage(descriptor, message);
            return;
        }

        if (descriptor.getProcessType() == IncomeDataProcessType.Immediate) {
            // Если обработка непосредственная, то прям в этом потоке вызываем обработчик(и) события.
            this.eventPublisher.publishEvent(message);
        } else {
            // Перед тем, как положить в очередь требуется дождаться "зеленного сигнала".
            var sleepMs = 1;
            while (!this.messagesQueue.allowPush()) {
                Thread.sleep(sleepMs);
                if (sleepMs < MAX_SLEEP_MS) {
                    sleepMs *= 2;
                }
            }
            // Собственно только теперь бросаем событие в очередь
            this.messagesQueue.pushMessage(descriptor.getPriority(), message);
        }
    }

    /**
     * Обработка фактов пропуска сообщений. В этом случае требуется периодически сдвигать offset,
     * но при этом надо это сделать аккуратно, чтобы не сохранить смещение после
     * @param descriptor Описатель канала
     * @param message Сообщение, которое пропускаем
     */
    protected void internalSkippedMessage(
            @NotNull final KafkaIncomeTopicLoadingDescriptor descriptor,
            @NotNull final Message<?> message
    ) {
        // TODO: ...
    }

    /**
     * Получение данных из Consumer-а.
     *
     * @param descriptor Описатель загрузки из Топика.
     * @return Записи Consumer-а.
     */
    @SuppressWarnings({"unchecked", "SynchronizationOnLocalVariableOrMethodParameter"})
    @NotNull
    protected ConsumerRecords<Object, Object> internalPoll(
            @NotNull final KafkaIncomeTopicLoadingDescriptor descriptor
    ) {
        ConsumerRecords<Object, Object> records;
        final var consumer = descriptor.getConsumer();
        synchronized (consumer) {
            records = (ConsumerRecords<Object, Object>) consumer
                    .poll(descriptor.getDurationOnPoll());
        }
        log.debug("Topic: {}; polled: {} records", descriptor.getChannelName(), records.count());
        return records;
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
