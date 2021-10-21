package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.offsets.TopicPartitionOffset;

import java.util.Collection;

/**
 * Задачи класса, реализующего данный интерфейс состоят в том, чтобы уметь читать стартовые смещения из
 * места источника хранения (это может быть локальная БД, а может быть и внешний сервис, предоставляющий данную услугу).
 */
@SuppressWarnings("unused")
public interface IncomeTopicsOffsetsLoader {
    /**
     * Загрузка смещений и выдача результата в виде коллекции.
     * @param configuration Конфигурация, для топиков которой требуется загрузить смещения.
     * @return Список смещений.
     */
    Collection<TopicPartitionOffset> loadOffsets(@NotNull final IncomeTopicsConfiguration configuration);

    /**
     * Загрузка смещений и применение их с помощью контроллера смещений.
     * @param configuration Конфигурация, для топиков которой требуется загрузить смещения.
     * @param offsetsController Контроллер смещения. С его помощью смещения будут применяться.
     */
    void loadAndSeekOffsets(@NotNull final IncomeTopicsConfiguration configuration, @NotNull final IncomeTopicsOffsetsController offsetsController);
}
