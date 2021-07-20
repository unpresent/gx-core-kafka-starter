package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.IncomeTopicsConsumingException;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.configuration.IncomeTopicsConfiguration;
import ru.gxfin.common.kafka.events.AbstractObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEventsFactory;

import java.time.Duration;
import java.util.ArrayList;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@Slf4j
public abstract class AbstractIncomeTopicsLoader
        implements IncomeTopicsLoader, ObjectsLoadedFromIncomeTopicEventsFactory {
    private final ApplicationContext context;

    protected AbstractIncomeTopicsLoader(ApplicationContext context) {
        super();
        this.context = context;
    }

    /**
     * Чтение набора DataPackage-ей из очереди.
     * @param topic2MemoryRepository    Описатель обработчика одной очереди.
     * @param durationOnPoll            Длительность ожидания данных в очереди.
     * @return                          Набор DataPackage-ей из очереди.
     * @throws JsonProcessingException  Ошибки при десериализации из Json-а.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Iterable<DataPackage> loadPackages(IncomeTopicLoadingDescriptor topic2MemoryRepository, Duration durationOnPoll) throws JsonProcessingException {
        if (topic2MemoryRepository.getMessageMode() != TopicMessageMode.PACKAGE) {
            throw new IncomeTopicsConsumingException("Can't load packages from topic: " + topic2MemoryRepository.getTopic());
        }
        final var records = internalPoll(topic2MemoryRepository, durationOnPoll);
        if (records.isEmpty()) {
            return null;
        }

        final var memoryRepository = topic2MemoryRepository.getMemoryRepository();
        final var result = new ArrayList<DataPackage>();
        for (var rec : records) {
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var pack = memoryRepository.loadPackage(valueString);
                result.add(pack);
            } else {
                throw new IncomeTopicsConsumingException("Unsupported value type received by consumer! Topic: " + topic2MemoryRepository.getTopic());
            }
        }
        return result;
    }

    /**
     * Чтение набора DataObject-ов из очереди.
     * @param topic2MemoryRepository    Описатель обработчика одной очереди.
     * @param durationOnPoll            Длительность ожидания данных в очереди.
     * @return                          Набор DataObject-ов из очереди.
     * @throws JsonProcessingException  Ошибки при десериализации из Json-а.
     */
    @Override
    public Iterable<DataObject> loadObjects(IncomeTopicLoadingDescriptor topic2MemoryRepository, Duration durationOnPoll) throws JsonProcessingException {
        if (topic2MemoryRepository.getMessageMode() != TopicMessageMode.OBJECT) {
            throw new IncomeTopicsConsumingException("Can't load objects from topic: " + topic2MemoryRepository.getTopic());
        }
        final var records = internalPoll(topic2MemoryRepository, durationOnPoll);
        if (records.isEmpty()) {
            return null;
        }

        final var memoryRepository = topic2MemoryRepository.getMemoryRepository();
        final var result = new ArrayList<DataObject>();
        for (var rec : records) {
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var obj = memoryRepository.loadObject(valueString);
                result.add(obj);
            } else {
                throw new IncomeTopicsConsumingException("Unsupported value type received by consumer! Topic: " + topic2MemoryRepository.getTopic());
            }
        }
        return result;
    }

    /**
     * Чтение объектов из очередей в порядке орпделенной в конфигурации.
     * @param configuration Конфигурация топиков.
     */
    public void loadTopicsByConfiguration(IncomeTopicsConfiguration configuration, Duration durationOnPoll) throws JsonProcessingException {
        final var pCount = configuration.prioritiesCount();
        for (int p = 0; p < pCount; p++) {
            var priorityObjectsCount = 0;
            final var topicDescriptors = configuration.getByPriority(p);
            for (var topicDescriptor : topicDescriptors) {
                log.debug("Loading working data from topic: {}", topicDescriptor.getTopic());
                var n = internalLoadTopic(topicDescriptor, durationOnPoll, configuration);
                priorityObjectsCount += n;
                log.debug("Loaded working data from topic: {}; {} objects", topicDescriptor.getTopic(), n);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected int internalLoadTopic(IncomeTopicLoadingDescriptor topic, Duration durationOnPoll, ObjectsLoadedFromIncomeTopicEventsFactory eventsFactory) throws JsonProcessingException {
        var result = 0;
        Iterable<DataObject> objects;
        if (topic.getMessageMode() == TopicMessageMode.OBJECT) {
            objects = this.loadObjects(topic, durationOnPoll);
            if (objects == null) {
                return 0;
            }
            for (var o : objects) {
                result++;
            }
        } else /*if (topic.getMessageMode() == TopicMessageMode.PACKAGE)*/ {
            final var packages = this.loadPackages(topic, durationOnPoll);
            final var objectsList = new ArrayList<DataObject>();
            if (packages == null) {
                return 0;
            }
            for (var pack : packages) {
                result += pack.size();
                objectsList.addAll(pack.getObjects());
            }
            objects = objectsList;
        }

        final var event = eventsFactory.getOrCreateEvent(topic.getOnLoadedEventClass(), this, topic, objects);
        if (event != null) {
            // Вызываем обработчик события о чтении объектов
            this.context.publishEvent(event);
        }

        return result;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected ConsumerRecords<Object, Object> internalPoll(IncomeTopicLoadingDescriptor topic2MemoryRepository, Duration durationOnPoll) {
        final var consumer = topic2MemoryRepository.getConsumer();
        final ConsumerRecords<Object, Object> records = consumer.poll(durationOnPoll);
        log.debug("Topic: {}; polled: {} records", topic2MemoryRepository.getTopic(), records.count());
        return records;
    }

    @Override
    public AbstractObjectsLoadedFromIncomeTopicEvent getOrCreateEvent(Class<ObjectsLoadedFromIncomeTopicEvent> eventClass, Object source, IncomeTopicLoadingDescriptor loadingDescriptor, Iterable<DataObject> objects) {
        return null;
    }
}
