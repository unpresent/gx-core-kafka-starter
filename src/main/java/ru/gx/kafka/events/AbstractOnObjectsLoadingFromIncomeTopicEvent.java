package ru.gx.kafka.events;

import lombok.*;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationEvent;
import ru.gx.kafka.load.IncomeTopicsLoaderContinueMode;
import ru.gx.data.DataObject;
import ru.gx.data.DataPackage;
import ru.gx.kafka.load.IncomeTopicLoadingDescriptor;

import java.util.Collection;

import static lombok.AccessLevel.*;

@Getter
@Setter(PROTECTED)
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@ToString
public abstract class AbstractOnObjectsLoadingFromIncomeTopicEvent<O extends DataObject, P extends DataPackage<O>>
        extends ApplicationEvent
        implements OnObjectsLoadingFromIncomeTopicEvent<O, P> {

    /**
     * Описатель загрузки из Топика.
     */
    @NotNull
    private IncomeTopicLoadingDescriptor<O, P> loadingDescriptor;

    /**
     * Список изменений.
     */
    @NotNull
    private Collection<NewOldDataObjectsPair<O>> changes;

    /**
     * Режим продолжения обработки других Топиков.
     */
    @NotNull
    private IncomeTopicsLoaderContinueMode continueMode;

    public AbstractOnObjectsLoadingFromIncomeTopicEvent(Object source) {
        super(source);
    }

    @SuppressWarnings("UnusedReturnValue")
    @NotNull
    public AbstractOnObjectsLoadingFromIncomeTopicEvent<O, P> reset(@NotNull final Object source, @NotNull final IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, @NotNull final Collection<NewOldDataObjectsPair<O>> changes) {
        super.source = source;
        return this
                .setLoadingDescriptor(loadingDescriptor)
                .setChanges(changes)
                .setContinueMode(IncomeTopicsLoaderContinueMode.Auto);
    }
}
