package ru.gxfin.common.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import ru.gxfin.common.data.AbstractMemRepo;

import java.util.Properties;

/**
 * Интерфейс конфигурации обработки входящих очередей.
 */
public interface IncomeTopicsConfiguration {
    /**
     * Полчение описателя обработчика по топику.
     * @param topic Имя топика, для которого требуется получить описатель.
     * @return Описатель обработчика одной очереди.
     */
    IncomeTopic2MemRepo get(String topic);

    /**
     * Регистрация описателя обработчика одной очереди.
     * @param priority Приоритет очереди.
     * @param topic Имя топика очереди.
     * @param consumer Объект-получатель.
     * @param memRepo Репозиторий, в который будут загружены входящие объекты.
     * @param mode Режим данных в очереди: Пообъектно и пакетно.
     * @return this.
     */
    IncomeTopicsConfiguration register(int priority, String topic, Consumer consumer, AbstractMemRepo memRepo, TopicMessageMode mode);

    /**
     * Регистрация описателя обработчика одной очереди.
     * @param priority Приоритет очереди.
     * @param topic Имя топика очереди.
     * @param memRepo Репозиторий, в который будут загружены входящие объекты.
     * @param mode Режим данных в очереди: Пообъектно и пакетно.
     * @param consumerProperties Свойства consumer-а.
     * @param partitions Разделы в топике.
     * @return this.
     */
    IncomeTopicsConfiguration register(int priority, String topic, AbstractMemRepo memRepo, TopicMessageMode mode, Properties consumerProperties, int... partitions);

    /**
     * Регистрация описателя обработчика одной очереди.
     * @param item Описатель обработчика одной очереди.
     * @return this.
     */
    IncomeTopicsConfiguration register(IncomeTopic2MemRepo item);

    /**
     * Дерегистрация обработчика очереди.
     * @param topic Имя топика очереди.
     * @return this.
     */
    IncomeTopicsConfiguration unregister(String topic);

    /**
     * @return Количество приоритетов.
     */
    int prioritiesCount();

    /**
     * Получение списка описателей обработчиков очередей по приоритету.
     * @param priority Приоритет.
     * @return Список описателей обработчиков.
     */
    Iterable<IncomeTopic2MemRepo> getByPriority(int priority);
}
