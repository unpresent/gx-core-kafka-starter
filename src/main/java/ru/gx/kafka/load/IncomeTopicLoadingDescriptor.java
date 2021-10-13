package ru.gx.kafka.load;

import lombok.*;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.ApplicationContext;
import ru.gx.data.DataMemoryRepository;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.TopicMessageMode;
import ru.gx.kafka.events.OnObjectsLoadedFromIncomeTopicEvent;
import ru.gx.kafka.events.OnObjectsLoadingFromIncomeTopicEvent;

import java.lang.reflect.ParameterizedType;
import java.security.InvalidParameterException;
import java.util.*;

import static lombok.AccessLevel.*;

/**
 * Описатель обработчика одной очереди.
 */
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class IncomeTopicLoadingDescriptor<O extends DataObject, P extends DataPackage<O>> {
    /**
     * Имя топика очереди.
     */
    @Getter
    @NotNull
    private final String topic;

    /**
     * Приоритет, с которым надо обрабатывать очередь.
     * 0 - высший.
     * > 0 - менее приоритетный.
     */
    @Getter
    @Setter
    private int priority;

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @Getter
    @Setter
    @NotNull
    private TopicMessageMode messageMode;

    /**
     * Объект-получатель сообщений.
     */
    @Getter
    // @NotNull
    private Consumer<?, ?> consumer;

    /**
     * Список смещений для каждого Partition-а.
     * Key - Partition.
     * Value - Offset.
     */
    @NotNull
    private final Map<Integer, Long> partitionOffsets = new HashMap<>();

    @SuppressWarnings("unused")
    public Collection<Integer> getPartitions() {
        return this.partitionOffsets.keySet();
    }

    @SuppressWarnings("unused")
    public long getOffset(int partition) {
        return this.partitionOffsets.get(partition);
    }

    public void setOffset(int partition, long offset) {
        this.partitionOffsets.put(partition, offset);
    }

    /**
     * Получение коллекции TopicPartition. Формируется динамически. Изменять данную коллекцию нет смысла!
     *
     * @return Коллекция TopicPartition-ов.
     */
    public Collection<TopicPartition> getTopicPartitions() {
        final var result = new ArrayList<TopicPartition>();
        this.partitionOffsets
                .keySet()
                .forEach(p -> result.add(new TopicPartition(getTopic(), p)));
        return result;
    }

    /**
     * Из списка {@link #partitionOffsets} удаляем те, где key не в списке {@param partitions}.
     * И добавляем в {@link #partitionOffsets} новые записи с key = p, value = -1 (т.к. не знаем смещения).
     * Если в списке смещений, уже есть такой partition, то его не трогаем.
     *
     * @param partitions Список Partition-ов, который должен быть у нас.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "Convert2MethodRef"})
    public IncomeTopicLoadingDescriptor<O, P> setPartitions(int... partitions) {
        // Готовим список ключей для удаления - такие PartitionOffset-ы, которых нет в списке partitions:
        final var keyForRemove = new ArrayList<Integer>();
        this.partitionOffsets.keySet().stream()
                .filter(pkey -> Arrays.stream(partitions).noneMatch(p -> p == pkey))
                .forEach(pkey -> keyForRemove.add(pkey));

        // Удаляем:
        for (var k : keyForRemove) {
            this.partitionOffsets.remove(k);
        }

        // Добавляем только, если нет:
        Arrays.stream(partitions).forEach(p -> {
            if (!this.partitionOffsets.containsKey(p)) {
                this.partitionOffsets.put(p, (long) -1);
            }
        });

        return this;
    }

    /**
     * Установка смещения для Партиции очереди.
     *
     * @param partition Партиция.
     * @param offset    Смещение.
     */
    public void setDeserializedPartitionOffset(int partition, long offset) {
        this.partitionOffsets.put(partition, offset);
    }

    /**
     * Репозиторий, в который будут загружены входящие объекты.
     */
    @Getter
    @Setter
    @Nullable
    private DataMemoryRepository<O, P> memoryRepository;

    /**
     * Класс объектов, которые будут читаться из очереди.
     */
    @Getter
    private Class<? extends O> dataObjectClass;

    /**
     * Класс пакетов объектов, которые будут читаться из очереди.
     */
    @Getter
    private Class<? extends P> dataPackageClass;

    /**
     * Класс объектов-событий при загрузке объектов - запрос с предоставлением списка Old-New.
     */
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadingFromIncomeTopicEvent<O, P>> onLoadingEventClass;

    /**
     * Класс (не определенный с точностью до Dto-объекта) объектов-событий при загрузке объектов - запрос с предоставлением списка Old-New.
     */
    @SuppressWarnings("rawtypes")
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadingFromIncomeTopicEvent> onLoadingEventBaseClass;

    @SuppressWarnings("rawtypes")
    public OnObjectsLoadingFromIncomeTopicEvent getOnLoadingEvent(@NotNull ApplicationContext context) {
        if (this.onLoadingEventClass != null) {
            return context.getBean(onLoadingEventClass);
        } else if (this.onLoadingEventBaseClass != null) {
            return context.getBean(onLoadingEventBaseClass);
        }
        return null;
    }

    /**
     * Класс объектов-событий после чтения объектов (и загрузки в репозиторий).
     */
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadedFromIncomeTopicEvent<O, P>> onLoadedEventClass;

    /**
     * Класс (не определенный с точностью до Dto-объекта) объектов-событий после чтения объектов (и загрузки в репозиторий).
     */
    @SuppressWarnings("rawtypes")
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadedFromIncomeTopicEvent> onLoadedEventBaseClass;

    @SuppressWarnings("rawtypes")
    public OnObjectsLoadedFromIncomeTopicEvent getOnLoadedEvent(@NotNull final ApplicationContext context) {
        if (this.onLoadedEventClass != null) {
            return context.getBean(onLoadedEventClass);
        } else if (this.onLoadedEventBaseClass != null) {
            return context.getBean(onLoadedEventBaseClass);
        }
        return null;
    }

    /**
     * Режим чтения данных из очереди (с сохранением в репозиторий, без сохранения).
     */
    @Getter
    @Setter
    @NotNull
    private LoadingMode loadingMode;

    @Getter
    @Setter
    @Nullable
    private LoadingFiltering loadingFiltering;

    /**
     * Статистика чтения и обработки данных.
     */
    @Getter
    @NotNull
    private final IncomeTopicLoadingStatistics loadingStatistics = new IncomeTopicLoadingStatistics();

    /**
     * @return Строка с информацией об обработанных PartitionOffset-ах для логирования.
     */
    public String getDeserializedPartitionsOffsetsForLog() {
        StringBuilder result = new StringBuilder();
        for (final var p : this.partitionOffsets.keySet()) {
            if (result.length() > 0) {
                result.append("; ");
            }
            result
                    .append(p.toString())
                    .append(':')
                    .append(this.partitionOffsets.get(p));
        }
        return result.toString();
    }

    /**
     * Признак того, что описатель инициализирован.
     */
    @Getter
    private boolean initialized;

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @param consumerProperties Свойства consumer-а, который будет создан.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "unused"})
    @NotNull
    public IncomeTopicLoadingDescriptor<O, P> init(@NotNull final Properties consumerProperties) throws InvalidParameterException {
        if (this.dataObjectClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[0].");
        }
        if (this.dataPackageClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[1].");
        }

        if (this.partitionOffsets.size() <= 0) {
            throw new InvalidParameterException("Not defined partitions for topic " + this.topic);
        }
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.consumer.assign(getTopicPartitions());
        this.initialized = true;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public IncomeTopicLoadingDescriptor<O, P> unInit() {
        this.initialized = false;
        this.consumer = null;
        return this;
    }

    @SuppressWarnings({"unused", "unchecked"})
    public IncomeTopicLoadingDescriptor(@NotNull final String topic, @Nullable final IncomeTopicLoadingDescriptorsDefaults defaults) {
        this.topic = topic;

        final var thisClass = this.getClass();
        final var superClass = thisClass.getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            this.dataObjectClass = (Class<O>) ((ParameterizedType) superClass).getActualTypeArguments()[0];
            this.dataPackageClass = (Class<P>) ((ParameterizedType) superClass).getActualTypeArguments()[1];
        }

        if (defaults != null) {
            this
                    .setLoadingMode(defaults.getLoadingMode())
                    .setPartitions(defaults.getPartitions())
                    .setMessageMode(defaults.getTopicMessageMode());
        }
    }
}
