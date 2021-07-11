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
     * @param topic2MemRepo Описатель обработчика одной очереди.
     * @param durationOnPoll Длительность ожидания данных в очереди.
     * @return Набор DataPackage-ей из очереди.
     * @throws JsonProcessingException
     */
    @Override
    public Iterable<DataPackage> loadPackages(IncomeTopic2MemRepo topic2MemRepo, Duration durationOnPoll) throws JsonProcessingException {
        if (topic2MemRepo.getMessageMode() != TopicMessageMode.PACKAGE) {
            throw new IncomeTopicsConsumingException("Can't load packages from topic: " + topic2MemRepo.getTopic());
        }
        final var records = internalPoll(topic2MemRepo, durationOnPoll);
        if (records.isEmpty()) {
            return null;
        }

        final var memRepo = topic2MemRepo.getMemRepo();
        final var result = new ArrayList<DataPackage>();
        for (var rec : records) {
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var pack = memRepo.deserializePackage(valueString);
                result.add(pack);
            } else {
                throw new IncomeTopicsConsumingException("Unsupported value type received by consumer! Topic: " + topic2MemRepo.getTopic());
            }
        }
        return result;
    }

    /**
     * Чтение набора DataObject-ов из очереди.
     * @param topic2MemRepo Описатель обработчика одной очереди.
     * @param durationOnPoll Длительность ожидания данных в очереди.
     * @return Набор DataObject-ов из очереди.
     * @throws JsonProcessingException
     */
    @Override
    public Iterable<DataObject> loadObjects(IncomeTopic2MemRepo topic2MemRepo, Duration durationOnPoll) throws JsonProcessingException {
        if (topic2MemRepo.getMessageMode() != TopicMessageMode.OBJECT) {
            throw new IncomeTopicsConsumingException("Can't load objects from topic: " + topic2MemRepo.getTopic());
        }
        final var records = internalPoll(topic2MemRepo, durationOnPoll);
        if (records.isEmpty()) {
            return null;
        }

        final var memRepo = topic2MemRepo.getMemRepo();
        final var result = new ArrayList<DataObject>();
        for (var rec : records) {
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var obj = memRepo.deserializeObject(valueString);
                result.add(obj);
            } else {
                throw new IncomeTopicsConsumingException("Unsupported value type received by consumer! Topic: " + topic2MemRepo.getTopic());
            }
        }
        return result;
    }


    protected ConsumerRecords<Object, Object> internalPoll(IncomeTopic2MemRepo topic2MemRepo, Duration durationOnPoll) {
        final var consumer = topic2MemRepo.getConsumer();
        final ConsumerRecords<Object, Object> records = consumer.poll(durationOnPoll);
        log.debug("Topic: {}; polled: {} records", topic2MemRepo.getTopic(), records.count());
        return records;
    }
}
