package ru.gxfin.common.kafka;

/**
 * Режим представления данных в очереди - пообъектно или пакетно.
 */
public enum TopicMessageMode {
    /**
     * В очереди каждое сообщение - это отдельный объект.
     */
    OBJECT,

    /**
     * В очереди каждое сообщение - это пакет объектов.
     */
    PACKAGE
}
