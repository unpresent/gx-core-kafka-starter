package ru.gxfin.common.kafka.loader;

@SuppressWarnings("unused")
public enum LoadingMode {
    /**
     * Если репозиторий указан, то == UpdateInRepository; Если не указан, то ничего
     */
    Auto,

    /**
     * Только добавление новых объектов.
     */
    OnlyNewObjectsToRepository,

    /**
     * Обновление на новые версии объектов в репозитории.
     */
    UpdateInRepository,

    /**
     * Замена на новые версии объектов в репозитории.
     */
    ReplaceInRepository,

    /**
     * Регистрация или обновление объектов в репозитории будет производиться вручную или не будет вообще производиться.
     */
    ManualPutToRepository
}
