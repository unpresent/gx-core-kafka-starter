package ru.gx.kafka.offsets;

import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.TopicDirection;

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
    Collection<TopicPartitionOffset> loadOffsets(@NotNull final TopicDirection direction, @NotNull final String serviceName);
}
