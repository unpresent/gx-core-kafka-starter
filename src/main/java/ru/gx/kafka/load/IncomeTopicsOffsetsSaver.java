package ru.gx.kafka.load;

import org.jetbrains.annotations.NotNull;
import ru.gx.kafka.offsets.TopicPartitionOffset;

import java.util.Collection;

/**
 * Задачи класса, реализующего данный интерфейс состоят в том, чтобы уметь сохранять текущие смещения в
 * места постоянного хранения (это может быть локальная БД, а может быть и внешний сервис, предоставляющий данную услугу).
 */
@SuppressWarnings("unused")
public interface IncomeTopicsOffsetsSaver {
    /**
     * Сохранение всех смещений, переданным в списке смещений.
     * @param offsets Список смещений, который требуется сохранить.
     */
    void saveOffsets(@NotNull final String readerName, @NotNull final Collection<TopicPartitionOffset> offsets);

    /**
     * Сохранение всех текущих смещений описателей в конфигурации.
     * @param configuration Конфигурация, для описателей которой требуется сохранить смещения.
     */
    void saveOffsetsOfConfiguration(@NotNull final IncomeTopicsConfiguration configuration);
}
