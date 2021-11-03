package ru.gx.kafka.load;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.ApplicationContext;
import ru.gx.data.DataMemoryRepository;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.events.OnObjectsLoadedFromIncomeTopicEvent;
import ru.gx.kafka.events.OnObjectsLoadingFromIncomeTopicEvent;

import java.lang.reflect.ParameterizedType;
import java.security.InvalidParameterException;
import java.util.Properties;

/**
 * Описатель обработчика одной очереди.
 */
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class StandardIncomeTopicLoadingDescriptor<O extends DataObject, P extends DataPackage<O>> extends AbstractIncomeTopicLoadingDescriptor {
    /**
     * Репозиторий, в который будут загружены входящие объекты.
     */
    @Getter
    @Setter
    @Nullable
    private DataMemoryRepository<O, P> memoryRepository;

    /**
     * Класс объектов, которые будут читаться из очереди.
     */
    @Getter
    @Setter
    private Class<? extends O> dataObjectClass;

    /**
     * Класс пакетов объектов, которые будут читаться из очереди.
     */
    @Getter
    @Setter
    private Class<? extends P> dataPackageClass;

    /**
     * Класс объектов-событий при загрузке объектов - запрос с предоставлением списка Old-New.
     */
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadingFromIncomeTopicEvent<O, P>> onLoadingEventClass;

    /**
     * Класс (не определенный с точностью до Dto-объекта) объектов-событий при загрузке объектов - запрос с предоставлением списка Old-New.
     */
    @SuppressWarnings("rawtypes")
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadingFromIncomeTopicEvent> onLoadingEventUntypedClass;

    @SuppressWarnings("rawtypes")
    public OnObjectsLoadingFromIncomeTopicEvent getOnLoadingEvent(@NotNull ApplicationContext context) {
        if (this.onLoadingEventClass != null) {
            return context.getBean(onLoadingEventClass);
        } else if (this.onLoadingEventUntypedClass != null) {
            return context.getBean(onLoadingEventUntypedClass);
        }
        return null;
    }

    /**
     * Класс объектов-событий после чтения объектов (и загрузки в репозиторий).
     */
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadedFromIncomeTopicEvent<O, P>> onLoadedEventClass;

    /**
     * Класс (не определенный с точностью до Dto-объекта) объектов-событий после чтения объектов (и загрузки в репозиторий).
     */
    @SuppressWarnings("rawtypes")
    @Getter
    @Setter
    @Nullable
    private Class<? extends OnObjectsLoadedFromIncomeTopicEvent> onLoadedEventUntypedClass;

    @SuppressWarnings("rawtypes")
    public OnObjectsLoadedFromIncomeTopicEvent getOnLoadedEvent(@NotNull final ApplicationContext context) {
        if (this.onLoadedEventClass != null) {
            return context.getBean(onLoadedEventClass);
        } else if (this.onLoadedEventUntypedClass != null) {
            return context.getBean(onLoadedEventUntypedClass);
        }
        return null;
    }

    /**
     * Настройка Descriptor-а должна заканчиваться этим методом.
     *
     * @param consumerProperties Свойства consumer-а, который будет создан.
     * @return this.
     */
    @SuppressWarnings({"UnusedReturnValue", "unused"})
    @NotNull
    public StandardIncomeTopicLoadingDescriptor<O, P> init(@NotNull final Properties consumerProperties) throws InvalidParameterException {
        if (this.dataObjectClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[0].");
        }
        if (this.dataPackageClass == null) {
            throw new InvalidParameterException("Can't init descriptor " + this.getClass().getSimpleName() + " due undefined generic parameter[1].");
        }
        super.init(consumerProperties);
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    @Override
    @NotNull
    public StandardIncomeTopicLoadingDescriptor<O, P> unInit() {
        super.unInit();
        return this;
    }

    @SuppressWarnings({"unused", "unchecked"})
    protected StandardIncomeTopicLoadingDescriptor(@NotNull AbstractIncomeTopicsConfiguration owner, @NotNull final String topic, @Nullable final IncomeTopicLoadingDescriptorsDefaults defaults) {
        super(owner, topic, defaults);

        final var thisClass = this.getClass();
        final var superClass = thisClass.getGenericSuperclass();
        if (superClass instanceof ParameterizedType) {
            this.dataObjectClass = (Class<O>) ((ParameterizedType) superClass).getActualTypeArguments()[0];
            this.dataPackageClass = (Class<P>) ((ParameterizedType) superClass).getActualTypeArguments()[1];
        }
    }
}
