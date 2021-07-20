package ru.gxfin.common.kafka.events;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.context.ApplicationEvent;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.IncomeTopicsLoaderContinueMode;
import ru.gxfin.common.kafka.loader.IncomeTopicLoadingDescriptor;

@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractObjectsLoadedFromIncomeTopicEvent<O extends DataObject>
        extends ApplicationEvent
        implements ObjectsLoadedFromIncomeTopicEvent<O> {

    @Getter
    @Setter(AccessLevel.PROTECTED)
    private IncomeTopicLoadingDescriptor loadingDescriptor;

    @Getter
    @Setter(AccessLevel.PROTECTED)
    private Iterable<O> objects;

    @Getter
    @Setter(AccessLevel.PROTECTED)
    private IncomeTopicsLoaderContinueMode continueMode;

    public AbstractObjectsLoadedFromIncomeTopicEvent(Object source) {
        super(source);
    }

    @SuppressWarnings("UnusedReturnValue")
    public AbstractObjectsLoadedFromIncomeTopicEvent<O> reset(Object source, IncomeTopicLoadingDescriptor<O> loadingDescriptor, Iterable<O> objects) {
        super.source = source;
        return this
                .setLoadingDescriptor(loadingDescriptor)
                .setObjects(objects)
                .setContinueMode(IncomeTopicsLoaderContinueMode.Auto);
    }
}
