package ru.gxfin.common.kafka.events;

public enum ActionOnChangingDueLoading {
    /**
     * Ничего не делать: если нет репозитория, или объекты совпадают, или просто проигнорировать более новую версию.
     */
    Nothing,

    /**
     * Обновить в репозитории объект, значениями нового объекта.
     */
    Update,

    /**
     * Заменить в репозитории объект новым.
     */
    ReplaceOrInsert
}
