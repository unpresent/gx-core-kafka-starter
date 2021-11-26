package ru.gx.core.kafka.offsets;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.channels.ChannelDirection;

import java.util.Collection;

/**
 * Задачи класса, реализующего данный интерфейс состоят в том, чтобы уметь читать стартовые смещения из
 * места источника хранения (это может быть локальная БД, а может быть и внешний сервис, предоставляющий данную услугу).
 */
@SuppressWarnings("unused")
public interface TopicsOffsetsLoader {
    /**
     * Загрузка смещений и выдача результата в виде коллекции.
     * @param direction Направление топиков, для которых надо прочитать смещения.
     * @param serviceName Название сервиса, для которого надо прочитать смещения.
     * @return Список смещений.
     */
    Collection<TopicPartitionOffset> loadOffsets(@NotNull final ChannelDirection direction, @NotNull final String serviceName);
}
