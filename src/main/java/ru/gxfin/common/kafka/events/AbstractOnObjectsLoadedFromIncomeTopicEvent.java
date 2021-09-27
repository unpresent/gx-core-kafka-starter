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
public abstract class AbstractOnObjectsLoadedFromIncomeTopicEvent<O extends DataObject, P extends DataPackage<O>>
        extends ApplicationEvent
        implements OnObjectsLoadedFromIncomeTopicEvent<O, P> {

    /**
     * Описатель загрузки из Топика.
     */
    private IncomeTopicLoadingDescriptor<O, P> loadingDescriptor;

    /**
     * Список объектов, которые были загружены.
     */
    private Collection<O> objects;

    /**
     * Режим продолжения обработки других Топиков.
     */
    private IncomeTopicsLoaderContinueMode continueMode;

    public AbstractOnObjectsLoadedFromIncomeTopicEvent(Object source) {
        super(source);
    }

    @SuppressWarnings("UnusedReturnValue")
    @NotNull
    public AbstractOnObjectsLoadedFromIncomeTopicEvent<O, P> reset(Object source, @NotNull IncomeTopicLoadingDescriptor<O, P> loadingDescriptor, @NotNull Collection<O> objects) {
        super.source = source;
        return this
                .setLoadingDescriptor(loadingDescriptor)
                .setObjects(objects)
                .setContinueMode(IncomeTopicsLoaderContinueMode.Auto);
    }
}
