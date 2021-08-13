package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.data.ObjectAlreadyExistsException;
import ru.gxfin.common.data.ObjectNotExistsException;
import ru.gxfin.common.kafka.IncomeTopicsConsumingException;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.configuration.IncomeTopicsConfiguration;
import ru.gxfin.common.kafka.events.ActionOnChangingDueLoading;
import ru.gxfin.common.kafka.events.NewOldDataObjectsPair;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@Slf4j
public abstract class AbstractIncomeTopicsLoader implements IncomeTopicsLoader {
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Fields">
    /**
     * Объект контекста требуется для вызова событий.
     */
    private final ApplicationContext context;

    /**
     * ObjectMapper требуется для десериализации данных в объекты.
     */
    private final ObjectMapper objectMapper;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractIncomeTopicsLoader(ApplicationContext context, ObjectMapper objectMapper) {
        super();
        this.context = context;
        this.objectMapper = objectMapper;
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsLoader">

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public int processByTopic(IncomeTopicLoadingDescriptor descriptor, Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        final var statistics = descriptor.getLoadingStatistics().reset();
        var msStart = System.currentTimeMillis();
        // Получаем данные из очереди.
        final var loadedObjects = internalLoadObjectsFromTopic(descriptor, durationOnPoll);
        var msLoaded = System.currentTimeMillis();

        // Формируем список изменений
        final var changes = prepareChanges(descriptor, loadedObjects);
        var msChanges = System.currentTimeMillis();

        // Бросаем событие об изменениях со списком
        final var eventLoading = descriptor.getOnLoadingEvent(this.context);
        if (eventLoading != null) {
            eventLoading.reset(this, descriptor, changes);
            this.context.publishEvent(eventLoading);
        }
        var msEventLoading = System.currentTimeMillis();

        // Загружаем в репозиторий
        loadToRepository(descriptor, changes);
        var msLoadedToRepository = System.currentTimeMillis();

        // Формируем список объектов результата работы
        final var resultObjects = new ArrayList<DataObject>();
        changes.forEach(ch ->
                resultObjects.add(ch.getNewObject())
        );

        // Бросаем событие о завершении загрузки
        final var eventLoaded = descriptor.getOnLoadedEvent(this.context);
        if (eventLoaded != null) {
            eventLoaded.reset(this, descriptor, resultObjects);
            this.context.publishEvent(eventLoaded);
        }

        var msFinish = System.currentTimeMillis();

        statistics
                .setLoadedToRepositoryMs(msLoaded - msStart)
                .setPrepareChangesMs(msChanges - msLoaded)
                .setOnLoadingEventMs(msEventLoading - msChanges)
                .setLoadedToRepositoryMs(msLoadedToRepository - msEventLoading)
                .setOnLoadedEventMs(msFinish - msLoadedToRepository);

        log.info("Loaded from topic {} (offset: {}), {} objects, {} packages [inserted: {}, updated: {}, replaced{}] in {} ms (loading: {}, changes: {}, eventLoading: {}, repository: {}, eventLoaded: {}).",
                descriptor.getTopic(),
                descriptor.getDeserializedPartitionsOffsetsForLog(),
                statistics.getLoadedObjectsCount(),
                statistics.getLoadedPackagesCount(),
                statistics.getInsertedCount(),
                statistics.getUpdatedCount(),
                statistics.getReplacedCount(),
                msFinish - msStart,
                statistics.getLoadedFromKafkaMs(),
                statistics.getPrepareChangesMs(),
                statistics.getOnLoadingEventMs(),
                statistics.getLoadedToRepositoryMs(),
                statistics.getOnLoadedEventMs()
        );

        return loadedObjects.size();
    }

    /**
     * Чтение объектов из очередей в порядке определенной в конфигурации.
     *
     * @param configuration Конфигурация топиков.
     */
    public void processTopicsByConfiguration(IncomeTopicsConfiguration configuration, Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        final var pCount = configuration.prioritiesCount();
        for (int p = 0; p < pCount; p++) {
            final var topicDescriptors = configuration.getByPriority(p);
            for (var topicDescriptor : topicDescriptors) {
                log.debug("Loading working data from topic: {}", topicDescriptor.getTopic());
                var n = processByTopic(topicDescriptor, durationOnPoll);
                log.debug("Loaded working data from topic: {}; {} objects", topicDescriptor.getTopic(), n);
            }
        }
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя реализация">

    /**
     * Чтение набора DataPackage-ей из очереди.
     *
     * @param descriptor     Описатель обработчика одной очереди.
     * @param durationOnPoll Длительность ожидания данных в очереди.
     * @return Набор DataPackage-ей из очереди.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Collection<DataPackage> internalLoadPackages(IncomeTopicLoadingDescriptor descriptor, Duration durationOnPoll) throws JsonProcessingException {
        if (descriptor.getMessageMode() != TopicMessageMode.PACKAGE) {
            throw new IncomeTopicsConsumingException("Can't load packages from topic: " + descriptor.getTopic());
        }
        final var records = internalPoll(descriptor, durationOnPoll);
        if (records.isEmpty()) {
            return null;
        }

        final var result = new ArrayList<DataPackage>();
        for (var rec : records) {
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var pack = (DataPackage) this.objectMapper.readValue(valueString, descriptor.getDataPackageClass());
                result.add(pack);
            } else {
                throw new IncomeTopicsConsumingException("Unsupported value type received by consumer! Topic: " + descriptor.getTopic());
            }
            descriptor.setDeserializedPartitionOffset(rec.partition(), rec.offset());
        }
        return result;
    }

    /**
     * Чтение набора DataObject-ов из очереди.
     *
     * @param descriptor     Описатель обработчика одной очереди.
     * @param durationOnPoll Длительность ожидания данных в очереди.
     * @return Набор DataObject-ов из очереди.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Collection<DataObject> internalLoadObjects(IncomeTopicLoadingDescriptor descriptor, Duration durationOnPoll) throws JsonProcessingException {
        if (descriptor.getMessageMode() != TopicMessageMode.OBJECT) {
            throw new IncomeTopicsConsumingException("Can't load objects from topic: " + descriptor.getTopic());
        }
        final var records = internalPoll(descriptor, durationOnPoll);
        if (records.isEmpty()) {
            return null;
        }

        final var result = new ArrayList<DataObject>();
        for (var rec : records) {
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var obj = (DataObject) this.objectMapper.readValue(valueString, descriptor.getDataObjectClass());
                result.add(obj);
            } else {
                throw new IncomeTopicsConsumingException("Unsupported value type received by consumer! Topic: " + descriptor.getTopic());
            }
            descriptor.setDeserializedPartitionOffset(rec.partition(), rec.offset());
        }
        return result;
    }

    /**
     * Получение списка объектов данных из Топика. Если данные в топике лежат пакетами,
     * то объекты из пакетов извлекаются в результирующую коллекцию.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @param durationOnPoll Длительность, в течение которой ожидать данных из Топика.
     * @return Список объектов данных.
     * @throws JsonProcessingException Ошибка десериализации из Json-а.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Collection<DataObject> internalLoadObjectsFromTopic(IncomeTopicLoadingDescriptor descriptor, Duration durationOnPoll) throws JsonProcessingException {
        Collection<DataObject> objects;
        if (descriptor.getMessageMode() == TopicMessageMode.OBJECT) {
            objects = this.internalLoadObjects(descriptor, durationOnPoll);
            if (objects == null) {
                return null;
            }
            descriptor.getLoadingStatistics()
                    .setLoadedObjectsCount(objects.size());
        } else /*if (topic.getMessageMode() == TopicMessageMode.PACKAGE)*/ {
            var packagesCount = 0;
            var count = 0;
            final var packages = this.internalLoadPackages(descriptor, durationOnPoll);
            final var objectsList = new ArrayList<DataObject>();
            if (packages == null) {
                return null;
            }
            for (var pack : packages) {
                packagesCount++;
                count += pack.size();
                objectsList.addAll(pack.getObjects());
            }
            // TODO: Переделать сборку списка(?) объектов в более элегантное решение
            objects = objectsList;
            descriptor.getLoadingStatistics()
                    .setLoadedPackagesCount(packagesCount)
                    .setLoadedObjectsCount(count);
        }
        return objects;
    }

    /**
     * Получение данных из Consumer-а.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @param durationOnPoll Длительность, в течение которой ожидать данных из Топика.
     * @return Записи Consumer-а.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected ConsumerRecords<Object, Object> internalPoll(IncomeTopicLoadingDescriptor descriptor, Duration durationOnPoll) {
        final var consumer = descriptor.getConsumer();
        final ConsumerRecords<Object, Object> records = consumer.poll(durationOnPoll);
        log.debug("Topic: {}; polled: {} records", descriptor.getTopic(), records.count());
        return records;
    }

    /**
     * Получение списка изменений
     *
     * @param descriptor Описатель загрузки из Топика.
     * @param loaded     Коллекция загруженных из Топика объектов.
     * @return Коллекция описателей изменений.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected Collection<NewOldDataObjectsPair<DataObject>> prepareChanges(IncomeTopicLoadingDescriptor descriptor, Collection<DataObject> loaded) {
        final var result = new ArrayList<NewOldDataObjectsPair<DataObject>>();
        final var memRepo = descriptor.getMemoryRepository();
        if (descriptor.getOnLoadingEventClass() != null && memRepo != null) {
            loaded.forEach(newObj -> {
                final var key = memRepo.extractKey(newObj);
                final var oldObj = memRepo.getByKey(key);
                final ActionOnChangingDueLoading act;
                if (descriptor.getLoadingMode() == LoadingMode.ManualPutToRepository) {
                    // Т.к. заказали вручную обновление репозитория, то предлагаем Ничего не делать для этой пары.
                    act = ActionOnChangingDueLoading.Nothing;
                } else if (oldObj == null) {
                    // Т.к. это новый объект, то все равно какой режим загрузки, требуется добавить новый объект - предлагаем добавление.
                    act = ActionOnChangingDueLoading.ReplaceOrInsert;
                } else if (oldObj.equals(newObj)) {
                    // Т.к. новый объект не отличается от старого, то предлагаем оставить старый объект.
                    act = ActionOnChangingDueLoading.ReplaceOrInsert;
                } else {
                    switch (descriptor.getLoadingMode()) {
                        case Auto:
                        case UpdateInRepository:
                            act = ActionOnChangingDueLoading.Update;
                            break;
                        case ReplaceInRepository:
                            act = ActionOnChangingDueLoading.ReplaceOrInsert;
                            break;
                        default:
                            act = ActionOnChangingDueLoading.Nothing;
                    }
                }
                result.add(new NewOldDataObjectsPair<>(key, newObj, oldObj, act));
            });
        } else {
            loaded.forEach(newObj ->
                    result.add(new NewOldDataObjectsPair<>(null, newObj, null, ActionOnChangingDueLoading.Nothing))
            );
        }

        descriptor.getLoadingStatistics().setChangesCount(result.size());
        return result;
    }

    /**
     * Загрузка изменений в репозиторий.
     *
     * @param descriptor Описатель загрузки из Топика.
     * @param changes    Коллекция описателей изменений.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void loadToRepository(IncomeTopicLoadingDescriptor descriptor, Collection<NewOldDataObjectsPair<DataObject>> changes) throws JsonMappingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        final var memRepo = descriptor.getMemoryRepository();
        if (memRepo == null) {
            return;
        }

        var inserted = 0;
        var updated = 0;
        var replaced = 0;

        for (var ch : changes) {
            if (ch.getAction() == ActionOnChangingDueLoading.ReplaceOrInsert) {
                if (ch.getOldObject() == null) {
                    memRepo.insert(ch.getKey(), ch.getNewObject());
                    inserted++;
                } else if (ch.getAction() == ActionOnChangingDueLoading.Update) {
                    memRepo.replace(ch.getKey(), ch.getNewObject());
                    replaced++;
                }
            } else if (ch.getAction() == ActionOnChangingDueLoading.Update) {
                memRepo.update(ch.getKey(), ch.getNewObject());
                updated++;
            }
        }

        descriptor.getLoadingStatistics()
                .setInsertedCount(inserted)
                .setUpdatedCount(updated)
                .setReplacedCount(replaced);
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
}
