package ru.gxfin.common.kafka.events;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.context.ApplicationEvent;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.IncomeTopicsLoaderContinueMode;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

import java.util.Collection;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractOnObjectsLoadedFromIncomeTopicEvent<O extends DataObject, P extends DataPackage<O>>
        extends ApplicationEvent
        implements OnObjectsLoadedFromIncomeTopicEvent<O, P> {

    /**
     * Описатель загрузки из Топика.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private IncomeTopicLoadingDescriptor<O, P> loadingDescriptor;

    /**
     * Список объектов, которые были загружены.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private Collection<O> objects;

    /**
     * Режим продолжения обработки других Топиков.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private IncomeTopicsLoaderContinueMode continueMode;

    public AbstractOnObjectsLoadedFromIncomeTopicEvent(Object source) {
        super(source);
    }

    @SuppressWarnings("UnusedReturnValue")
    public AbstractOnObjectsLoadedFromIncomeTopicEvent<O, P> reset(Object source, IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, Collection<O> objects) {
        super.source = source;
        return this
                .setLoadingDescriptor(loadingDescriptor)
                .setObjects(objects)
                .setContinueMode(IncomeTopicsLoaderContinueMode.Auto);
    }
}
