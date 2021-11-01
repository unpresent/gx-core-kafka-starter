package ru.gx.kafka.load;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.ApplicationContext;
import ru.gx.kafka.SerializeMode;
import ru.gx.kafka.TopicMessageMode;
import ru.gx.kafka.events.OnRawDataLoadedFromIncomeTopicEvent;
import ru.gx.kafka.upload.OutcomeTopicUploadingDescriptor;

import java.security.InvalidParameterException;
import java.util.Collection;
import java.util.Properties;

/**
 * Описатель обработчика одной очереди.
 */
@SuppressWarnings("UnusedReturnValue")
public interface IncomeTopicLoadingDescriptor {
    /**
     * Имя топика очереди.
     */
    @NotNull
    String getTopic();

    /**
     * Приоритет, с которым надо обрабатывать очередь.
     * 0 - высший.
     * > 0 - менее приоритетный.
     */
    int getPriority();

    /**
     * Установка приоритета у топика.
     * @param priority приоритет.
     * @return this.
     */
    @NotNull
    IncomeTopicLoadingDescriptor setPriority(int priority);

    /**
     * Режим данных в очереди: Пообъектно и пакетно.
     */
    @NotNull
    TopicMessageMode getMessageMode();

    /**
     * Установка режима данных в очереди.
     * @param messageMode режим данных в очереди.
     * @return this.
     */
    @NotNull
    IncomeTopicLoadingDescriptor setMessageMode(@NotNull final TopicMessageMode messageMode);

    /**
     * Режим сериализации: Строки или Байты.
     */
    @NotNull
    SerializeMode getSerializeMode();

    /**
     * @param serializeMode Режим сериализации: Строки или Байты.
     * @return this.
     */
    @NotNull
    IncomeTopicLoadingDescriptor setSerializeMode(@NotNull final SerializeMode serializeMode);


    /**
     * Объект-получатель сообщений.
     */
    @NotNull
    Consumer<?, ?> getConsumer();

    @NotNull
    Collection<Integer> getPartitions();

    long getOffset(final int partition);

    /**
     * Метод предназначен только для сохранения смещения.
     * @param partition раздел, по которому запоминаем смещение в описателе.
     * @param offset само смещение, которое запоминаем в описателе.
     */
    @NotNull
    IncomeTopicLoadingDescriptor setOffset(final int partition, final long offset);

    /**
     * Получение коллекции TopicPartition. Формируется динамически. Изменять данную коллекцию нет смысла!
     *
     * @return Коллекция TopicPartition-ов.
     */
    @NotNull
    Collection<TopicPartition> getTopicPartitions();

    /**
     * Полная перезапись списка Partition-ов.
     *
     * @param partitions Список Partition-ов, который должен быть у нас.
     * @return this.
     */
    @NotNull
    IncomeTopicLoadingDescriptor setPartitions(int... partitions);

    /**
     * Установка смещения для Партиции очереди.
     *
     * @param partition Партиция.
     * @param offset    Смещение.
     */
    @NotNull
    IncomeTopicLoadingDescriptor setProcessedPartitionOffset(int partition, long offset);

    /**
     * @return Строка с информацией об обработанных PartitionOffset-ах для логирования.
     */
    @NotNull
    String getProcessedPartitionsOffsetsInfoForLog();

    /**
     * Получение объекта-события после чтения объектов (и загрузки в репозиторий).
     */
    @Nullable
    OnRawDataLoadedFromIncomeTopicEvent getOnRawDataLoadedEvent(@NotNull final ApplicationContext context);

    /**
     * Режим чтения данных из очереди (с сохранением в репозиторий, без сохранения).
     */
    @NotNull
    LoadingMode getLoadingMode();

    @NotNull
    IncomeTopicLoadingDescriptor setLoadingMode(@NotNull final LoadingMode loadingMode);

    @Nullable
    LoadingFiltering getLoadingFiltering();

    @NotNull
    IncomeTopicLoadingDescriptor setLoadingFiltering(@Nullable final LoadingFiltering loadingFiltering);

    /**
     * Статистика чтения и обработки данных.
     */
    @NotNull
    IncomeTopicLoadingStatistics getLoadingStatistics();

    /**
     * Признак того, что описатель инициализирован.
     */
    boolean isInitialized();

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @param consumerProperties Свойства consumer-а, который будет создан.
     * @return this.
     */
    @NotNull
    IncomeTopicLoadingDescriptor init(@NotNull final Properties consumerProperties) throws InvalidParameterException;

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @return this.
     */
    @NotNull
    IncomeTopicLoadingDescriptor init() throws InvalidParameterException;

    IncomeTopicLoadingDescriptor unInit();
}
