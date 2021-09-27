package ru.gxfin.common.kafka.events;

import lombok.*;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationEvent;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.data.DataPackage;
import ru.gxfin.common.kafka.IncomeTopicsLoaderContinueMode;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

import java.util.Collection;

@Getter
@Setter(AccessLevel.PROTECTED)
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@ToString
public abstract class AbstractOnObjectsLoadingFromIncomeTopicEvent<O extends DataObject, P extends DataPackage<O>>
        extends ApplicationEvent
        implements OnObjectsLoadingFromIncomeTopicEvent<O, P> {

    /**
     * Описатель загрузки из Топика.
     */
    private IncomeTopicLoadingDescriptor<O, P> loadingDescriptor;

    /**
     * Список изменений.
     */
    private Collection<NewOldDataObjectsPair<O>> changes;

    /**
     * Режим продолжения обработки других Топиков.
     */
    private IncomeTopicsLoaderContinueMode continueMode;

    public AbstractOnObjectsLoadingFromIncomeTopicEvent(Object source) {
        super(source);
    }

    @SuppressWarnings("UnusedReturnValue")
    @NotNull
    public AbstractOnObjectsLoadingFromIncomeTopicEvent<O, P> reset(Object source, @NotNull IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, @NotNull Collection<NewOldDataObjectsPair<O>> changes) {
        super.source = source;
        return this
                .setLoadingDescriptor(loadingDescriptor)
                .setChanges(changes)
                .setContinueMode(IncomeTopicsLoaderContinueMode.Auto);
    }
}
