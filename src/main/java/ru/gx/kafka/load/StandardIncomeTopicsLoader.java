package ru.gx.kafka.load;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import ru.gx.kafka.ServiceHeadersKeys;
import ru.gx.kafka.TopicMessageMode;
import ru.gx.kafka.events.NewOldDataObjectsPair;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.data.ObjectAlreadyExistsException;
import ru.gx.data.ObjectNotExistsException;
import ru.gx.kafka.events.ActionOnChangingDueLoading;
import ru.gx.utils.BytesUtils;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.*;

import static lombok.AccessLevel.*;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@Slf4j
public class StandardIncomeTopicsLoader implements IncomeTopicsLoader, ApplicationContextAware {
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

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    public StandardIncomeTopicsLoader() {
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsLoader">

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @return Список загруженных объектов.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    @NotNull
    public Collection<Object> processByTopic(@NotNull final IncomeTopicLoadingDescriptor descriptor) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        checkDescriptorIsInitialized(descriptor);
        if (descriptor instanceof StandardIncomeTopicLoadingDescriptor) {
            return internalProcessByTopicStandardDescriptor((StandardIncomeTopicLoadingDescriptor) descriptor);
        } else if (descriptor instanceof RawDataIncomeTopicLoadingDescriptor) {
            return (Collection) internalProcessByTopicRawDataDescriptor((RawDataIncomeTopicLoadingDescriptor) descriptor);
        } else {
            throw new IncomeTopicsConfigurationException("Unsupported descriptor type " + descriptor.getClass().getName());
        }
    }

    /**
     * Чтение объектов из очередей в порядке определенной в конфигурации.
     *
     * @return Map-а, в которой для каждого дескриптора указан список загруженных объектов.
     */
    @Override
    @NotNull
    public Map<IncomeTopicLoadingDescriptor, Collection<Object>>
    processAllTopics(@NotNull final IncomeTopicsConfiguration configuration) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException, InvalidParameterException {
        final var pCount = configuration.prioritiesCount();
        final var result = new HashMap<IncomeTopicLoadingDescriptor, Collection<Object>>();
        for (int p = 0; p < pCount; p++) {
            final var topicDescriptors = configuration.getByPriority(p);
            if (topicDescriptors == null) {
                throw new IncomeTopicsConfigurationException("Invalid null value getByPriority(" + p + ")");
            }
            for (var topicDescriptor : topicDescriptors) {
                log.debug("Loading working data from topic: {}", topicDescriptor.getTopic());
                final var loadedObjects = processByTopic(topicDescriptor);
                result.put(topicDescriptor, loadedObjects);
                log.debug("Loaded working data from topic: {}; loadedObjects.size() == {}", topicDescriptor.getTopic(), loadedObjects.size());
            }
        }
        return result;
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя реализация">
    protected Collection<ConsumerRecord<?, ?>> internalProcessByTopicRawDataDescriptor(@NotNull final RawDataIncomeTopicLoadingDescriptor descriptor) {
        checkDescriptorIsInitialized(descriptor);

        final var statistics = descriptor.getLoadingStatistics().reset();
        var msStart = System.currentTimeMillis();
        // Получаем данные из очереди.
        final var loadedRecords = internalLoadRecordsFromTopic(descriptor);
        var msLoaded = System.currentTimeMillis();

        // Бросаем событие о загрузке сырых данных
        final var eventLoaded = descriptor.getOnRawDataLoadedEvent(this.applicationContext);
        if (eventLoaded != null) {
            eventLoaded.reset(this, descriptor, IncomeTopicsLoaderContinueMode.Auto, loadedRecords);
            this.applicationContext.publishEvent(eventLoaded);
        }

        var msFinish = System.currentTimeMillis();

        statistics
                .setLoadedToRepositoryMs(msLoaded - msStart)
                .setOnLoadedEventMs(msFinish - msLoaded);

        log.info("Loaded from topic {} (offset: {}), {} objects, {} packages in {} ms (L: {}, eLed: {}).",
                descriptor.getTopic(),
                descriptor.getProcessedPartitionsOffsetsInfoForLog(),
                statistics.getLoadedObjectsCount(),
                statistics.getLoadedPackagesCount(),
                msFinish - msStart,
                statistics.getLoadedFromKafkaMs(),
                statistics.getOnLoadedEventMs()
        );

        return loadedRecords;
    }

    protected <O extends DataObject, P extends DataPackage<O>>
    Collection<O> internalProcessByTopicStandardDescriptor(@NotNull final StandardIncomeTopicLoadingDescriptor<O, P> descriptor) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        checkDescriptorIsInitialized(descriptor);

        final var statistics = descriptor.getLoadingStatistics().reset();
        var msStart = System.currentTimeMillis();

        // TODO: Переработать на получение сырых данных из очереди, потом предоставление события,
        // потом уже десериализация и загрузка в MemoryRepo

        // Получаем данные из очереди.
        final var loadedObjects = internalLoadObjectsFromTopic(descriptor);
        var msLoaded = System.currentTimeMillis();

        // Формируем список изменений
        final var changes = prepareChanges(descriptor, loadedObjects);
        var msChanges = System.currentTimeMillis();

        // Бросаем событие об изменениях со списком
        final var eventLoading = descriptor.getOnLoadingEvent(this.applicationContext);
        if (eventLoading != null) {
            //noinspection unchecked
            eventLoading.reset(this, descriptor, changes);
            this.applicationContext.publishEvent(eventLoading);
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
        final var eventLoaded = descriptor.getOnLoadedEvent(this.applicationContext);
        if (eventLoaded != null) {
            //noinspection unchecked
            eventLoaded.reset(this, descriptor, resultObjects);
            this.applicationContext.publishEvent(eventLoaded);
        }

        var msFinish = System.currentTimeMillis();

        statistics
                .setLoadedToRepositoryMs(msLoaded - msStart)
                .setPrepareChangesMs(msChanges - msLoaded)
                .setOnLoadingEventMs(msEventLoading - msChanges)
                .setLoadedToRepositoryMs(msLoadedToRepository - msEventLoading)
                .setOnLoadedEventMs(msFinish - msLoadedToRepository);

        log.info("Loaded from topic {} (offset: {}), {} objects, {} packages [I:{}, U:{}, R:{}] in {} ms (L: {}, Ch: {}, eLing: {}, Rep: {}, eLed: {}).",
                descriptor.getTopic(),
                descriptor.getProcessedPartitionsOffsetsInfoForLog(),
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

        return loadedObjects;
    }

    /**
     * Проверка описателя на то, что прошла инициализация. Работать с неинициализированным описателем нельзя.
     *
     * @param descriptor описатель, который проверяем.
     */
    protected void checkDescriptorIsInitialized(@NotNull final IncomeTopicLoadingDescriptor descriptor) {
        if (!descriptor.isInitialized()) {
            throw new IncomeTopicsConfigurationException("Topic descriptor " + descriptor.getTopic() + " is not initialized!");
        }
    }

    /**
     * Получение списка объектов данных из Топика. Если данные в топике лежат пакетами,
     * то объекты из пакетов извлекаются в результирующую коллекцию.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @return Список объектов данных.
     * @throws JsonProcessingException Ошибка десериализации из Json-а.
     */
    protected <O extends DataObject, P extends DataPackage<O>>
    Collection<O> internalLoadObjectsFromTopic(@NotNull final StandardIncomeTopicLoadingDescriptor<O, P> descriptor) throws JsonProcessingException {
        Collection<O> objects;
        if (descriptor.getMessageMode() == TopicMessageMode.Object) {
            objects = this.internalLoadObjects(descriptor);
            if (objects == null) {
                return null;
            }
            descriptor.getLoadingStatistics()
                    .setLoadedObjectsCount(objects.size());
        } else /*if (topic.getMessageMode() == TopicMessageMode.Package)*/ {
            var packagesCount = 0;
            var count = 0;
            final var packages = this.internalLoadPackages(descriptor);
            final var objectsList = new ArrayList<O>();
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
     * Чтение набора DataPackage-ей из очереди.
     *
     * @param descriptor     Описатель обработчика одной очереди.
     * @return Набор DataPackage-ей из очереди.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    protected <O extends DataObject, P extends DataPackage<O>>
    Collection<P> internalLoadPackages(@NotNull final StandardIncomeTopicLoadingDescriptor<O, P> descriptor) throws JsonProcessingException {
        if (descriptor.getMessageMode() != TopicMessageMode.Package) {
            throw new IncomeTopicsConfigurationException("Can't load packages from topic: " + descriptor.getTopic());
        }

        final var result = new ArrayList<P>();
        final var records = internalPoll(descriptor);
        if (records.isEmpty()) {
            return result;
        }

        for (var rec : records) {
            // Фильтрация (если задана)
            if (descriptor.getLoadingFiltering() != null) {
                final var headers = rec.headers();
                if (!descriptor.getLoadingFiltering().allowProcess(headers)) {
                    continue;
                }
            }

            // Десериализация
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var pack = (P) this.objectMapper.readValue(valueString, descriptor.getDataPackageClass());
                result.add(pack);
            } else {
                throw new IncomeTopicsLoadingException("Unsupported value type received by consumer! Topic: " + descriptor.getTopic());
            }
            descriptor.setProcessedPartitionOffset(rec.partition(), rec.offset());
        }
        return result;
    }

    /**
     * Чтение набора DataObject-ов из очереди.
     *
     * @param descriptor     Описатель обработчика одной очереди.
     * @return Набор DataObject-ов из очереди.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    protected <O extends DataObject, P extends DataPackage<O>>
    Collection<O> internalLoadObjects(@NotNull final StandardIncomeTopicLoadingDescriptor<O, P> descriptor) throws JsonProcessingException {
        if (descriptor.getMessageMode() != TopicMessageMode.Object) {
            throw new IncomeTopicsConfigurationException("Can't load objects from topic: " + descriptor.getTopic());
        }

        final var result = new ArrayList<O>();
        final var records = internalPoll(descriptor);
        if (records.isEmpty()) {
            return result;
        }

        for (var rec : records) {
            // Фильтрация (если задана)
            if (descriptor.getLoadingFiltering() != null) {
                final var headers = rec.headers();
                if (!descriptor.getLoadingFiltering().allowProcess(headers)) {
                    continue;
                }
            }

            // Десериализация
            final var value = rec.value();
            if (value instanceof String) {
                final var valueString = (String) value;
                log.trace("Polled: {}", valueString);
                final var obj = (O) this.objectMapper.readValue(valueString, descriptor.getDataObjectClass());
                result.add(obj);
            } else {
                throw new IncomeTopicsLoadingException("Unsupported value type received by consumer! Topic: " + descriptor.getTopic());
            }
            descriptor.setProcessedPartitionOffset(rec.partition(), rec.offset());
        }
        return result;
    }

    /**
     * Получение списка Record-ов из Топика.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @return Список объектов данных.
     */
    protected Collection<ConsumerRecord<?, ?>> internalLoadRecordsFromTopic(@NotNull final RawDataIncomeTopicLoadingDescriptor descriptor) {
        final var result = new ArrayList<ConsumerRecord<?, ?>>();
        final var statistics = descriptor.getLoadingStatistics();

        final var records = internalPoll(descriptor);

        if (descriptor.getMessageMode() == TopicMessageMode.Object) {
            statistics.setLoadedObjectsCount(records.count());
            for (var rec : records) {
                result.add(rec);
                descriptor.setProcessedPartitionOffset(rec.partition(), rec.offset());
            }
        } else /*if (topic.getMessageMode() == TopicMessageMode.Package)*/ {
            statistics.setLoadedPackagesCount(records.count());
            var count = 0;
            for (var rec : records) {
                result.add(rec);
                descriptor.setProcessedPartitionOffset(rec.partition(), rec.offset());
                final var h = rec.headers().lastHeader(ServiceHeadersKeys.dataPackageSize);
                if (h != null) {
                    count += BytesUtils.bytesToLong(h.value());
                }
            }
            statistics.setLoadedObjectsCount(count);
        }
        return result;
    }


    /**
     * Получение данных из Consumer-а.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @return Записи Consumer-а.
     */
    @SuppressWarnings("unchecked")
    @NotNull
    protected ConsumerRecords<Object, Object> internalPoll(@NotNull final IncomeTopicLoadingDescriptor descriptor) {
        final var consumer = descriptor.getConsumer();
        final ConsumerRecords<Object, Object> records = (ConsumerRecords<Object, Object>) consumer.poll(descriptor.getDurationOnPoll());
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
    @NotNull
    protected <O extends DataObject, P extends DataPackage<O>> Collection<NewOldDataObjectsPair<O>> prepareChanges(@NotNull StandardIncomeTopicLoadingDescriptor<O, P> descriptor, @NotNull Collection<O> loaded) {
        final var result = new ArrayList<NewOldDataObjectsPair<O>>();
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
    protected <O extends DataObject, P extends DataPackage<O>> void loadToRepository(@NotNull StandardIncomeTopicLoadingDescriptor<O, P> descriptor, @NotNull Collection<NewOldDataObjectsPair<O>> changes) throws JsonMappingException, ObjectNotExistsException, ObjectAlreadyExistsException {
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
                    memRepo.insert(ch.getNewObject());
                    inserted++;
                } else if (ch.getAction() == ActionOnChangingDueLoading.Update) {
                    memRepo.replace(ch.getNewObject());
                    replaced++;
                }
            } else if (ch.getAction() == ActionOnChangingDueLoading.Update) {
                memRepo.update(ch.getNewObject());
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
