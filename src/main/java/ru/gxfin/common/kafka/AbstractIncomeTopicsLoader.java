package ru.gxfin.common.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;

import java.time.Duration;
import java.util.ArrayList;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@Slf4j
public abstract class AbstractIncomeTopicsLoader implements IncomeTopicsLoader {
    protected AbstractIncomeTopicsLoader() {
        super();
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
    public Iterable<DataPackage> loadPackages(IncomeTopic2MemoryRepository topic2MemoryRepository, Duration durationOnPoll) throws JsonProcessingException {
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
    public Iterable<DataObject> loadObjects(IncomeTopic2MemoryRepository topic2MemoryRepository, Duration durationOnPoll) throws JsonProcessingException {
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

    @SuppressWarnings("unchecked")
    protected ConsumerRecords<Object, Object> internalPoll(IncomeTopic2MemoryRepository topic2MemoryRepository, Duration durationOnPoll) {
        final var consumer = topic2MemoryRepository.getConsumer();
        final ConsumerRecords<Object, Object> records = consumer.poll(durationOnPoll);
        log.debug("Topic: {}; polled: {} records", topic2MemoryRepository.getTopic(), records.count());
        return records;
    }
}
