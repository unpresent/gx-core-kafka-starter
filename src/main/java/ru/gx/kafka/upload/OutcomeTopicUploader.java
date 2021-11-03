package ru.gx.kafka.upload;

import org.apache.kafka.common.header.Header;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.gx.kafka.offsets.PartitionOffset;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;

@SuppressWarnings("unused")
public interface OutcomeTopicUploader {

    /**
     * @param data выгружаемые данные: DataObject, Iterable<DataObject>, DataPackage или другого типа данные.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился объект.
     */
    @NotNull PartitionOffset uploadAnyData(
            @NotNull OutcomeTopicUploadingDescriptor descriptor,
            @NotNull Object data,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * @param object выгружаемый объект.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился объект.
     */
    @SuppressWarnings("rawtypes")
    @NotNull
    PartitionOffset uploadDataObject(
            @NotNull StandardOutcomeTopicUploadingDescriptor descriptor,
            @NotNull DataObject object,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * Выгрузить несколько объектов данных.
     * @param descriptor описатель исходящей очереди.
     * @param objects коллекция объектов.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @SuppressWarnings("rawtypes")
    @NotNull
    PartitionOffset uploadDataObjects(
            @NotNull StandardOutcomeTopicUploadingDescriptor descriptor,
            @NotNull Iterable<DataObject> objects,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * Выгрузить пакет объектов данных.
     * @param descriptor описатель исходящей очереди.
     * @param dataPackage пакет объектов.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @SuppressWarnings("rawtypes")
    @NotNull
    PartitionOffset uploadDataPackage(
            @NotNull StandardOutcomeTopicUploadingDescriptor descriptor,
            @NotNull DataPackage dataPackage,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * Выгрузить все объекты из MemoryRepository в данном описателе.
     * @param descriptor описатель исходящей очереди.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @SuppressWarnings("rawtypes")
    @NotNull
    PartitionOffset publishMemoryRepositorySnapshot(
            @NotNull StandardOutcomeTopicUploadingDescriptor descriptor,
            @Nullable Iterable<Header> headers) throws Exception;


    /**
     * Выгрузить все объекты из MemoryRepository в данном описателе.
     * @param descriptor описатель исходящей очереди.
     * @param snapshotOffAllObjects полный snapshot - должен быть список всех объектов.
     * @param headers заголовки.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @SuppressWarnings("rawtypes")
    @NotNull
    PartitionOffset publishFullSnapshot(
            @NotNull StandardOutcomeTopicUploadingDescriptor descriptor,
            @NotNull Iterable<DataObject> snapshotOffAllObjects,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * Получение offset-а последней выгрузки полного snapshot-а данных из MemoryRepository.
     * @param descriptor описатель исходящей очереди.
     * @return Смещение в очереди, с которым выгрузился первый объект в последнем snapshot-е.
     */
    @SuppressWarnings("rawtypes")
    @Nullable
    PartitionOffset getLastPublishedSnapshotOffset(
            @NotNull StandardOutcomeTopicUploadingDescriptor descriptor
    );
}
