package ru.gx.core.kafka.offsets;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.ChannelDirection;

import java.util.Collection;

/**
 * Задачи класса, реализующего данный интерфейс состоят в том, чтобы уметь сохранять текущие смещения в
 * места постоянного хранения (это может быть локальная БД, а может быть и внешний сервис, предоставляющий данную услугу).
 */
@SuppressWarnings("unused")
public interface TopicsOffsetsSaver {
    /**
     * Сохранение всех смещений, переданным в списке смещений.
     * @param offsets Список смещений, который требуется сохранить.
     */
    void saveOffsets(@NotNull final ChannelDirection direction, @NotNull final String serviceName, @NotNull final Collection<TopicPartitionOffset> offsets);
}
