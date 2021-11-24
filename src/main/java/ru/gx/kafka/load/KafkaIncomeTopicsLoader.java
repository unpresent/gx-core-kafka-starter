package ru.gx.kafka.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import ru.gx.channels.ChannelConfigurationException;
import ru.gx.channels.ChannelMessageMode;
import ru.gx.channels.IncomeDataProcessType;
import ru.gx.events.Event;
import ru.gx.events.EventsPrioritizedQueue;
import ru.gx.kafka.KafkaConstants;
import ru.gx.kafka.ServiceHeadersKeys;
import ru.gx.utils.BytesUtils;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

import static lombok.AccessLevel.PROTECTED;
import static lombok.AccessLevel.PUBLIC;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@Slf4j
public class KafkaIncomeTopicsLoader implements ApplicationContextAware {
    private final static int MAX_SLEEP_MS = 64;

    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Объект контекста требуется для вызова событий и для получения бинов(!).
     */
    @Getter(PROTECTED)
    @Setter(value = PUBLIC, onMethod_ = @Autowired)
    private ApplicationContext applicationContext;

    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ObjectMapper objectMapper;

    /**
     * Требуется для отправки сообщений в обработку.
     */
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private EventsPrioritizedQueue eventsQueue;

    /**
     * Требуется для отправки сообщений в обработку.
     */
    @Getter(PROTECTED)
    @Setter(value = PROTECTED, onMethod_ = @Autowired)
    private ApplicationEventPublisher eventPublisher;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public KafkaIncomeTopicsLoader() {
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsLoader">

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @return Список загруженных объектов.
     */
    public int processByTopic(@NotNull final KafkaIncomeTopicLoadingDescriptor<?, ?> descriptor) {
        checkDescriptorIsInitialized(descriptor);
        return internalProcessDescriptor(descriptor);
    }

    /**
     * Чтение объектов из очередей в порядке определенной в конфигурации.
     *
     * @return Map-а, в которой для каждого дескриптора указан список загруженных объектов.
     */
    @NotNull
    public Map<KafkaIncomeTopicLoadingDescriptor<?, ?>, Integer>
    processAllTopics(@NotNull final AbstractKafkaIncomeTopicsConfiguration configuration) throws InvalidParameterException {
        final var pCount = configuration.prioritiesCount();
        final var result = new HashMap<KafkaIncomeTopicLoadingDescriptor<?, ?>, Integer>();
        for (int p = 0; p < pCount; p++) {
            final var topicDescriptors = configuration.getByPriority(p);
            if (topicDescriptors == null) {
                throw new ChannelConfigurationException("Invalid null value getByPriority(" + p + ")");
            }
            for (var topicDescriptor : topicDescriptors) {
                if (topicDescriptor instanceof final KafkaIncomeTopicLoadingDescriptor<?, ?> kafkaDescriptor) {
                    log.debug("Loading working data from topic: {}", topicDescriptor.getName());
                    final var eventsCount = processByTopic(kafkaDescriptor);
                    result.put(kafkaDescriptor, eventsCount);
                    log.debug("Loaded working data from topic. Events: {}", kafkaDescriptor.getName());
                } else {
                    throw new ChannelConfigurationException("Invalid class of descriptor " + topicDescriptor.getName());
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
    protected void checkDescriptorIsInitialized(@NotNull final KafkaIncomeTopicLoadingDescriptor<?, ?> descriptor) {
        if (!descriptor.isInitialized()) {
            throw new ChannelConfigurationException("Channel descriptor " + descriptor.getName() + " is not initialized!");
        }
    }

    /**
     * Обработка входящих данных для указанного канала.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @return Список событий на обработку.
     */
    @SneakyThrows
    protected int internalProcessDescriptor(@NotNull final KafkaIncomeTopicLoadingDescriptor<?, ?> descriptor) {
        // TODO: Добавить сбор статистики
        final var records = internalPoll(descriptor);

        var eventsCount = 0;

        @SuppressWarnings("unused")
        var dataObjectsCount = 0; // Количество объектов в исходных данных. Для будущей статистики.

        if (descriptor.getMessageMode() == ChannelMessageMode.Object) {
            for (var rec : records) {
                internalProcessRecord(descriptor, rec);
                dataObjectsCount++;
                eventsCount++;
            }
        } else /*if (topic.getMessageMode() == TopicMessageMode.Package)*/ {
            for (var rec : records) {
                internalProcessRecord(descriptor, rec);
                final var h = rec.headers().lastHeader(ServiceHeadersKeys.dataPackageSize);
                if (h != null) {
                    dataObjectsCount += BytesUtils.bytesToLong(h.value());
                }
                eventsCount++;
            }
        }
        return eventsCount;
    }

    /**
     * Данный метод создает объект-событие, сохраняя в него данные и метаданные. Метаданные копируются из Header-ов сообщения,
     * а также добавляется смещение из Kafka, с которым была полученная запись ({@code record}).<br/>
     * Если в описателе канала {@code descriptor} указано, что обработка должна быть немедленной ({@link KafkaIncomeTopicLoadingDescriptor#getProcessType()}),
     * то событие бросается непосредственно в этом потоке.<br/>
     * Иначе пытаемся бросить событие в {@code eventsQueue}.
     * Перед вызовом {@link EventsPrioritizedQueue#pushEvent} сначала проверяем, можно ли в очередь положить событие:
     * {@link EventsPrioritizedQueue#allowPush()}
     * @param descriptor Описатель канала.
     * @param record Запись, полученная из Kafka.
     */
    @SuppressWarnings("BusyWait")
    @SneakyThrows(InterruptedException.class)
    protected void internalProcessRecord(@NotNull final KafkaIncomeTopicLoadingDescriptor<?, ?> descriptor, @NotNull final ConsumerRecord<Object, Object> record) {
        // Формируем объект-событие.
        final var event = descriptor
                .createEvent(this)
                .setData(record.value());

        // Копируем все Header-ы в Metadata.
        final var headers = record.headers();
        if (headers != null) {
            headers.forEach(h -> event.putMetadata(h.key(), h.value()));
        }

        // Чтобы при обработке бизнес данных можно было сохранить смещение, которое успешно обработано.
        event.putMetadata(KafkaConstants.METADATA_PARTITION, record.partition());
        event.putMetadata(KafkaConstants.METADATA_OFFSET, record.offset());

        if (descriptor.getProcessType() == IncomeDataProcessType.Immediate) {
            // Если обработка непосредственная, то прям в этом потоке вызываем обработчик(и) события.
            this.eventPublisher.publishEvent(event);
        } else {
            // Перед тем, как положить в очередь требуется дождаться "зеленного сигнала".
            var sleepMs = 1;
            while (!this.eventsQueue.allowPush()) {
                Thread.sleep(sleepMs);
                if (sleepMs < MAX_SLEEP_MS){
                    sleepMs *= 2;
                }
            }
            // Собственно только теперь бросаем событие в очередь
            this.eventsQueue.pushEvent(descriptor.getPriority(), event);
        }
    }

    /**
     * Получение данных из Consumer-а.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @return Записи Consumer-а.
     */
    @SuppressWarnings("unchecked")
    @NotNull
    protected ConsumerRecords<Object, Object> internalPoll(@NotNull final KafkaIncomeTopicLoadingDescriptor<?, ?> descriptor) {
        final var consumer = descriptor.getConsumer();
        final ConsumerRecords<Object, Object> records = (ConsumerRecords<Object, Object>) consumer.poll(descriptor.getDurationOnPoll());
        log.debug("Topic: {}; polled: {} records", descriptor.getName(), records.count());
        return records;
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
