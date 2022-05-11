package ru.gx.core.kafka.offsets;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.core.channels.AbstractChannelsConfiguration;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.messaging.Message;

import java.util.Collection;

/**
 * Задачи класса, реализующего данный интерфейс состоят в том, чтобы уметь читать стартовые смещения из
 * места источника хранения (это может быть локальная БД, а может быть и внешний сервис, предоставляющий данную услугу),
 * а также сохранять туда же текущие смещения.
 */
@SuppressWarnings("unused")
public interface TopicsOffsetsStorage {
    /**
     * Загрузка смещений и выдача результата в виде коллекции.
     *
     * @param direction   Направление топиков, для которых надо прочитать смещения.
     * @param serviceName Название сервиса, для которого надо прочитать смещения.
     * @return Список смещений.
     */
    @Nullable
    Collection<TopicPartitionOffset> loadOffsets(
            @NotNull final ChannelDirection direction,
            @NotNull final String serviceName,
            @NotNull final AbstractChannelsConfiguration configuration
    );

    /**
     * Сохранение всех смещений, переданным в списке смещений.
     *
     * @param direction   Направление (относительно текущего сервиса) очередей для сохраняемых смещений.
     * @param serviceName Имя текущего сервиса, для которого сохраняются смещения.
     * @param offsets     Список смещений, который требуется сохранить.
     */
    void saveOffsets(
            @NotNull final ChannelDirection direction,
            @NotNull final String serviceName,
            @NotNull final Collection<TopicPartitionOffset> offsets
    );

    /**
     * Сохранение всех смещений, переданным в списке смещений.
     *
     * @param direction   Направление (относительно текущего сервиса) очередей для сохраняемых смещений.
     * @param serviceName Имя текущего сервиса, для которого сохраняются смещения.
     * @param message     Список смещений извлекается из метаданных данного сообщения,
     *                    см. {@link ru.gx.core.kafka.KafkaConstants}
     */
    void saveOffsetFromMessage(
            @NotNull final ChannelDirection direction,
            @NotNull final String serviceName,
            @NotNull final Message<?> message
    );
}
