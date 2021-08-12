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
public abstract class AbstractOnObjectsLoadingFromIncomeTopicEvent<O extends DataObject, P extends DataPackage<O>>
        extends ApplicationEvent
        implements OnObjectsLoadingFromIncomeTopicEvent<O, P> {

    /**
     * Описатель загрузки из Топика.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private IncomeTopicLoadingDescriptor<O, P> loadingDescriptor;

    /**
     * Список изменений.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private Collection<NewOldDataObjectsPair<O>> changes;

    /**
     * Режим продолжения обработки других Топиков.
     */
    @Getter
    @Setter(AccessLevel.PROTECTED)
    private IncomeTopicsLoaderContinueMode continueMode;

    public AbstractOnObjectsLoadingFromIncomeTopicEvent(Object source) {
        super(source);
    }

    @SuppressWarnings("UnusedReturnValue")
    public AbstractOnObjectsLoadingFromIncomeTopicEvent<O, P> reset(Object source, IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, Collection<NewOldDataObjectsPair<O>> changes) {
        super.source = source;
        return this
                .setLoadingDescriptor(loadingDescriptor)
                .setChanges(changes)
                .setContinueMode(IncomeTopicsLoaderContinueMode.Auto);
    }
}
