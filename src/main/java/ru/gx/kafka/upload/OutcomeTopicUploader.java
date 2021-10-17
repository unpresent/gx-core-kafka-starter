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
     * @param object выгружаемый объект.
     * @param headers заголовки.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился объект.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataObject(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull O object,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * Выгрузить несколько объектов данных.
     * @param descriptor описатель исходящей очереди.
     * @param objects коллекция объектов.
     * @param headers заголовки.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataObjects(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull Iterable<O> objects,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * Выгрузить пакет объектов данных.
     * @param descriptor описатель исходящей очереди.
     * @param dataPackage пакет объектов.
     * @param headers заголовки.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset uploadDataPackage(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull P dataPackage,
            @Nullable Iterable<Header> headers) throws Exception;


    /**
     * Выгрузить все объекты из MemoryRepository в данном описателе.
     * @param descriptor описатель исходящей очереди.
     * @param headers заголовки.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset publishMemoryRepositorySnapshot(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @Nullable Iterable<Header> headers) throws Exception;


    /**
     * Выгрузить все объекты из MemoryRepository в данном описателе.
     * @param descriptor описатель исходящей очереди.
     * @param snapshotOffAllObjects полный snapshot - должен быть список всех объектов.
     * @param headers заголовки.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект.
     */
    @NotNull
    <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset publishFullSnapshot(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor,
            @NotNull Iterable<O> snapshotOffAllObjects,
            @Nullable Iterable<Header> headers) throws Exception;

    /**
     * Получение offset-а последней выгрузки полного snapshot-а данных из MemoryRepository.
     * @param descriptor описатель исходящей очереди.
     * @param <O> тип объекта.
     * @param <P> тип пакета объектов.
     * @return Смещение в очереди, с которым выгрузился первый объект в последнем snapshot-е.
     */
    @Nullable
    <O extends DataObject, P extends DataPackage<O>>
    PartitionOffset getLastPublishedSnapshotOffset(
            @NotNull OutcomeTopicUploadingDescriptor<O, P> descriptor
    );
}
