package ru.gxfin.common.kafka.loader;

import org.springframework.context.ApplicationContext;
import ru.gxfin.common.data.DataObject;
import ru.gxfin.common.kafka.events.AbstractObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;
import ru.gxfin.common.kafka.loader.AbstractIncomeTopicsLoader;

@SuppressWarnings("unused")
public class StandardIncomeTopicsLoader extends AbstractIncomeTopicsLoader {
    protected StandardIncomeTopicsLoader(ApplicationContext context) {
        super(context);
    }
}
