package ru.gxfin.common.kafka.loader;

import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import ru.gxfin.common.data.DataMemoryRepository;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.TopicMessageMode;

/**
 * Описатель обработчика одной очереди.
 */
public class IncomeTopicLoadingDescriptor<O extends DataObject> {
    /**
     * Имя топика очереди.
     */
    @Getter
    private final String topic;

    /**
     * Приоритет, с которым надо обрабтавать очередь.
     * 0 - высший.
     * > 0 - менее приоритетный.
     */
    @Getter
    private final int priority;

    /**
     * Объект-получатель сообщений.
     */
    @SuppressWarnings("rawtypes")
    @Getter
    private final Consumer consumer;

    /**
     * Репозиторий, в который будут загружены входящие объекты.
     */
    @Getter
    private final DataMemoryRepository<O> memoryRepository;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Getter
    private final TopicMessageMode messageMode;

    @Getter
    private final Class<? extends ObjectsLoadedFromIncomeTopicEvent<O>> onLoadedEventClass;

    @SuppressWarnings("rawtypes")
    public IncomeTopicLoadingDescriptor(
            String topic,
            int priority,
            Consumer consumer,
            DataMemoryRepository<O> memoryRepository,
            TopicMessageMode messageMode,
            Class<? extends ObjectsLoadedFromIncomeTopicEvent<O>> onLoadedEventClass
    ) {
        this.topic = topic;
        this.priority = priority;
        this.consumer = consumer;
        this.memoryRepository = memoryRepository;
        this.messageMode = messageMode;
        this.onLoadedEventClass = onLoadedEventClass;
    }
}
