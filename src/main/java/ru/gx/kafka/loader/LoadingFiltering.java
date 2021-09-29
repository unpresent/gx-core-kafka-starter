package ru.gx.kafka.loader;

import org.apache.kafka.common.header.Headers;
import org.jetbrains.annotations.NotNull;

/**
 * С помощью реализации данного интерфейса можно определять фильтрацию по загрузке данных.
 * На фильтрацию отдаются headers. Если данные требуется обрабатывать, то метод {@link #allowProcess(Headers)} должен вернуть true.
 */
public interface LoadingFiltering {
    /**
     * @param headers Заголовки полученных из Kafka данных.
     * @return  true - обрабатывать данные, false - проигнорировать данные.
     */
    boolean allowProcess(@NotNull final Headers headers);
}
