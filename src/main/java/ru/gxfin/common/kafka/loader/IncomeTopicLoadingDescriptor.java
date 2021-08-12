package ru.gxfin.common.kafka.loader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.DataMemoryRepository;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.configuration.IncomeTopicsConfiguration;
import ru.gxfin.common.kafka.events.OnObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.events.OnObjectsLoadingFromIncomeTopicEvent;

import java.lang.reflect.ParameterizedType;
import java.security.InvalidParameterException;
import java.util.*;

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
    private final String topic;

    /**
     * Приоритет, с которым надо обрабтавать очередь.
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
    private TopicMessageMode messageMode;

    /**
     * Объект-получатель сообщений.
     */
    @SuppressWarnings("rawtypes")
    @Getter
    private Consumer consumer;

    /**
     * Список смещений для каждого Partition-а.
     * Key - Partition.
     * Value - Offset.
     */
    @Getter
    private final Map<Integer, Long> partitionOffsets = new HashMap<>();

    /**
     * Получение коллекции TopicPartition. Формируется динамически. Изменять данную коллекцию нет смысла!
     * @return  Коллекция TopicPartition-ов.
     */
    public Collection<TopicPartition> getTopicPartitions() {
        final var result = new ArrayList<TopicPartition>();
        this.getPartitionOffsets()
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
    private DataMemoryRepository<O, P> memoryRepository;

    /**
     * Класс объектов, которые будут читаться из очереди.
     */
    @Getter
    @Setter
    private Class<? extends O> dataObjectClass;

    /**
     * Класс пакетов объектов, которые будут читаться из очереди.
     */
    @Getter
    @Setter
    private Class<? extends P> dataPackageClass;


    /**
     * Класс объектов-событий при загрузке объектов - запрос с предоставлением списка Old-New.
     */
    @Getter
    @Setter
    private Class<? extends OnObjectsLoadingFromIncomeTopicEvent<O, P>> onLoadingEventClass;

    public OnObjectsLoadingFromIncomeTopicEvent<O, P> getOnLoadingEvent(ApplicationContext context) {
        if (this.onLoadingEventClass != null) {
            return context.getBean(onLoadingEventClass);
        }
        return null;
    }


    /**
     * Класс объектов-событий после чтения объектов (и загрузки в репозиторий).
     */
    @Getter
    @Setter
    private Class<? extends OnObjectsLoadedFromIncomeTopicEvent<O, P>> onLoadedEventClass;

    public OnObjectsLoadedFromIncomeTopicEvent<O, P> getOnLoadedEvent(ApplicationContext context) {
        if (this.onLoadedEventClass != null) {
            return context.getBean(onLoadedEventClass);
        }
        return null;
    }

    /**
     * Режим чтения данных из очереди (с сохранением в репозиторий, без сохранения).
     */
    @Getter
    @Setter
    private LoadingMode loadingMode;

    @Getter
    private final IncomeTopicLoadingStatistics loadingStatistics = new IncomeTopicLoadingStatistics();

    /**
     * @return  Строка с информацией об обработанных PartitionOffset-ах для логирования.
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

    @Getter
    private boolean initialized;

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     * @param consumerProperties Свойства consumer-а, который будет создан.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "unused"})
    public IncomeTopicLoadingDescriptor<O, P> init(Properties consumerProperties) {
        if (partitionOffsets.size() <= 0) {
            throw new InvalidParameterException("Not defined partitions for topic " + this.topic);
        }

        this.consumer = new KafkaConsumer<>(consumerProperties);
        //noinspection unchecked
        this.consumer.assign(getTopicPartitions());
        this.initialized = true;
        return this;
    }

    @SuppressWarnings({"unused", "unchecked"})
    public IncomeTopicLoadingDescriptor(@NotNull IncomeTopicsConfiguration configuration, @NotNull String topic) {
        this.topic = topic;

        final var thisClass = this.getClass();
        final var superClass = thisClass.getGenericSuperclass();
        if (superClass != null) {
            // TODO: Проверить!
            this.dataObjectClass = (Class<O>) ((ParameterizedType) superClass).getActualTypeArguments()[0];
            this.dataPackageClass = (Class<P>) ((ParameterizedType) superClass).getActualTypeArguments()[1];
        }

        this
                .setLoadingMode(configuration.getDescriptorsDefaults().getLoadingMode())
                .setPartitions(configuration.getDescriptorsDefaults().getPartitions())
                .setMessageMode(configuration.getDescriptorsDefaults().getTopicMessageMode());
    }
}
