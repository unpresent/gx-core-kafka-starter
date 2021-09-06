package ru.gxfin.common.kafka.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.data.ObjectAlreadyExistsException;
import ru.gxfin.common.data.ObjectNotExistsException;
import ru.gxfin.common.kafka.IncomeTopicsConsumingException;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.events.ActionOnChangingDueLoading;
import ru.gxfin.common.kafka.events.NewOldDataObjectsPair;

import java.time.Duration;
import java.util.*;

/**
 * Базовая реализация загрузчика, который упрощает задачу чтения данных из очереди и десериалиазции их в объекты.
 */
@Slf4j
public abstract class AbstractIncomeTopicsLoader implements IncomeTopicsLoader, IncomeTopicsConfiguration {
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

    /**
     * Список описателей сгруппированные по приоритетам.
     */
    private final List<List<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>>> priorities = new ArrayList<>();

    /**
     * Список описателей с группировкой по топикам.
     */
    private final Map<String, IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> topics = new HashMap<>();

    @Getter
    private final IncomeTopicLoadingDescriptorsDefaults descriptorsDefaults;

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Initialization">
    protected AbstractIncomeTopicsLoader(ApplicationContext context, ObjectMapper objectMapper) {
        super();
        this.context = context;
        this.objectMapper = objectMapper;
        this.descriptorsDefaults = new IncomeTopicLoadingDescriptorsDefaults();
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsConfiguration">

    /**
     * Получение описателя обработчика по топику.
     *
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <O extends DataObject, P extends DataPackage<O>> IncomeTopicLoadingDescriptor<O, P> get(String topic) {
        return (IncomeTopicLoadingDescriptor<O, P>)this.topics.get(topic);
    }

    /**
     * Регистрация описателя обработчика одной очереди.
     *
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    @Override
    public <O extends DataObject, P extends DataPackage<O>> IncomeTopicsConfiguration register(IncomeTopicLoadingDescriptor<O, P> item) {
        if (this.topics.containsKey(item.getTopic())) {
            throw new IncomeTopicsConsumingException("Topic " + item.getTopic() + " already registered!");
        }

        if (!item.isInitialized()) {
            item.init(getDescriptorsDefaults().getConsumerProperties());
        }

        final var priority = item.getPriority();
        while (priorities.size() <= priority) {
            priorities.add(new ArrayList<>());
        }

        final var itemsList = priorities.get(priority);

        final var localItem = (IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>)item;
        itemsList.add(localItem);
        topics.put(item.getTopic(), localItem);

        return this;
    }

    /**
     * Дерегистрация обработчика очереди.
     *
     * @param topic Имя топика очереди.
     * @return this.
     */
    @Override
    public IncomeTopicsConfiguration unregister(String topic) {
        final var item = this.topics.get(topic);
        if (item == null) {
            throw new IncomeTopicsConsumingException("Topic " + topic + " not registered!");
        }

        this.topics.remove(topic);
        for (var pList : this.priorities) {
            if (pList.remove(item)) {
                break;
            }
        }

        return this;
    }

    /**
     * @return Количество приоритетов.
     */
    @Override
    public int prioritiesCount() {
        return this.priorities.size();
    }

    /**
     * Получение списка описателей обработчиков очередей по приоритету.
     *
     * @param priority Приоритет.
     * @return Список описателей обработчиков.
     */
    @Override
    public Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getByPriority(int priority) {
        return this.priorities.get(priority);
    }

    /**
     * @return Список всех описателей обработчиков очередей.
     */
    @Override
    public Iterable<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>> getAll() {
        return this.topics.values();
    }

    /**
     * Требование о смещении Offset-ов на начало для всех Topic-ов и всех Partition-ов.
     */
    @Override
    public void seekAllToBegin() {
        this.topics.values().forEach(this::internalSeekTopicAllPartitionsToBegin);
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Topic-ов и всех Partition-ов.
     */
    @Override
    public void seekAllToEnd() {
        this.topics.values().forEach(this::internalSeekTopicAllPartitionsToEnd);
    }

    /**
     * Требование о смещении Offset-ов на начало для всех Partition-ов для заданного Topic-а.
     *
     * @param topic Топик, для которого требуется сместить смещения.
     */
    @Override
    public void seekTopicAllPartitionsToBegin(String topic) {
        final IncomeTopicLoadingDescriptor<?, ?> topicDescriptor = this.get(topic);
        internalSeekTopicAllPartitionsToBorder(topicDescriptor, Consumer::seekToBeginning);
    }

    /**
     * Требование о смещении Offset-ов на конец для всех Partition-ов для заданного Topic-а.
     *
     * @param topic Топик, для которого требуется сместить смещения.
     */
    @Override
    public void seekTopicAllPartitionsToEnd(String topic) {
        final var topicDescriptor = this.get(topic);
        internalSeekTopicAllPartitionsToBorder(topicDescriptor, Consumer::seekToEnd);
    }

    /**
     * Требование о смещении Offset-ов на заданные значения для заданного Topic-а.
     *
     * @param topic            Топик, для которого требуется сместить смещения.
     * @param partitionOffsets Смещения (для каждого Partition-а свой Offset).
     */
    @Override
    public void seekTopic(String topic, Iterable<PartitionOffset> partitionOffsets) {
        final var topicDescriptor = this.get(topic);
        final var consumer = topicDescriptor.getConsumer();
        partitionOffsets
                .forEach(po -> {
                    final var tp = new TopicPartition(topic, po.getPartition());
                    consumer.seek(tp, po.getOffset() > 0 ? po.getOffset() : 0);
                    topicDescriptor.setOffset(tp.partition(), consumer.position(tp));
                });
    }
    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="реализация IncomeTopicsLoader">

    /**
     * Загрузка и обработка данных по списку топиков по конфигурации.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @param durationOnPoll Длительность, в течение которой ожидать данных из Топика.
     * @return Список загруженных объектов.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    @Override
    public <O extends DataObject, P extends DataPackage<O>> Collection<O> processByTopic(IncomeTopicLoadingDescriptor<O, P> descriptor, Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException {
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
        final var resultObjects = new ArrayList<O>();
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

        log.info("Loaded from topic {} (offset: {}), {} objects, {} packages [I:{}, U:{}, R:{}] in {} ms (L: {}, Ch: {}, eLing: {}, Rep: {}, eLed: {}).",
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

        return loadedObjects;
    }

    /**
     * Чтение объектов из очередей в порядке определенной в конфигурации.
     *
     * @return Map-а, в которой для каждого дескриптора указан список загруженных объектов.
     */
    public Map<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>, Collection<DataObject>> processAllTopics(Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        final var pCount = this.prioritiesCount();
        final var result = new HashMap<IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>>, Collection<DataObject>>();
        for (int p = 0; p < pCount; p++) {
            final var topicDescriptors = this.getByPriority(p);
            for (var topicDescriptor : topicDescriptors) {
                log.debug("Loading working data from topic: {}", topicDescriptor.getTopic());
                final var loadedObjects = invokeProcessByTopic(topicDescriptor, durationOnPoll);
                result.put(topicDescriptor, loadedObjects);
                log.debug("Loaded working data from topic: {}; {} objects", topicDescriptor.getTopic(), loadedObjects.size());
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <O extends DataObject, P extends DataPackage<O>> Collection<O> invokeProcessByTopic(IncomeTopicLoadingDescriptor<? extends DataObject, ? extends DataPackage<DataObject>> descriptor, Duration durationOnPoll) throws JsonProcessingException, ObjectNotExistsException, ObjectAlreadyExistsException {
        final IncomeTopicLoadingDescriptor<O, P> localDescriptor = (IncomeTopicLoadingDescriptor<O, P>)descriptor;
        return this.processByTopic(localDescriptor, durationOnPoll);
    }

    // </editor-fold>
    // -------------------------------------------------------------------------------------------------------------
    // <editor-fold desc="Внутренняя реализация">
    protected void internalSeekTopicAllPartitionsToBegin(IncomeTopicLoadingDescriptor<?, ?> topicDescriptor) {
        this.internalSeekTopicAllPartitionsToBorder(topicDescriptor, Consumer::seekToBeginning);
    }

    protected void internalSeekTopicAllPartitionsToEnd(IncomeTopicLoadingDescriptor<?, ?> topicDescriptor) {
        this.internalSeekTopicAllPartitionsToBorder(topicDescriptor, Consumer::seekToEnd);
    }

    protected void internalSeekTopicAllPartitionsToBorder(IncomeTopicLoadingDescriptor<?, ?> topicDescriptor, ConsumerSeekToBorderFunction func) {
        final Collection<TopicPartition> topicPartitions = topicDescriptor.getTopicPartitions();
        final var consumer = topicDescriptor.getConsumer();
        // consumer.seekToBeginning(topicPartitions);
        func.seek(consumer, topicPartitions);
        for (var tp : topicPartitions) {
            final var position = consumer.position(tp);
            topicDescriptor.setOffset(tp.partition(), position);
        }
    }


    /**
     * Чтение набора DataPackage-ей из очереди.
     *
     * @param descriptor     Описатель обработчика одной очереди.
     * @param durationOnPoll Длительность ожидания данных в очереди.
     * @return Набор DataPackage-ей из очереди.
     * @throws JsonProcessingException Ошибки при десериализации из Json-а.
     */
    protected <O extends DataObject, P extends DataPackage<O>> Collection<P> internalLoadPackages(IncomeTopicLoadingDescriptor<O, P> descriptor, Duration durationOnPoll) throws JsonProcessingException {
        if (descriptor.getMessageMode() != TopicMessageMode.PACKAGE) {
            throw new IncomeTopicsConsumingException("Can't load packages from topic: " + descriptor.getTopic());
        }

        final var result = new ArrayList<P>();
        final var records = internalPoll(descriptor, durationOnPoll);
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
    protected <O extends DataObject, P extends DataPackage<O>> Collection<O> internalLoadObjects(IncomeTopicLoadingDescriptor<O, P> descriptor, Duration durationOnPoll) throws JsonProcessingException {
        if (descriptor.getMessageMode() != TopicMessageMode.OBJECT) {
            throw new IncomeTopicsConsumingException("Can't load objects from topic: " + descriptor.getTopic());
        }

        final var result = new ArrayList<O>();
        final var records = internalPoll(descriptor, durationOnPoll);
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
    protected <O extends DataObject, P extends DataPackage<O>> Collection<O> internalLoadObjectsFromTopic(IncomeTopicLoadingDescriptor<O, P> descriptor, Duration durationOnPoll) throws JsonProcessingException {
        Collection<O> objects;
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
     * Получение данных из Consumer-а.
     *
     * @param descriptor     Описатель загрузки из Топика.
     * @param durationOnPoll Длительность, в течение которой ожидать данных из Топика.
     * @return Записи Consumer-а.
     */
    @SuppressWarnings("unchecked")
    protected <O extends DataObject, P extends DataPackage<O>> ConsumerRecords<Object, Object> internalPoll(IncomeTopicLoadingDescriptor<O, P> descriptor, Duration durationOnPoll) {
        final var consumer = descriptor.getConsumer();
        final ConsumerRecords<Object, Object> records = (ConsumerRecords<Object, Object>) consumer.poll(durationOnPoll);
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
    protected <O extends DataObject, P extends DataPackage<O>> Collection<NewOldDataObjectsPair<O>> prepareChanges(IncomeTopicLoadingDescriptor<O, P> descriptor, Collection<O> loaded) {
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
    protected <O extends DataObject, P extends DataPackage<O>> void loadToRepository(IncomeTopicLoadingDescriptor<O, P> descriptor, Collection<NewOldDataObjectsPair<O>> changes) throws JsonMappingException, ObjectNotExistsException, ObjectAlreadyExistsException {
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
