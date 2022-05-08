package ru.gx.core.kafka.listener;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.worker.AbstractOnStoppingExecuteEvent;

public class KafkaSimpleListenerOnStoppingExecuteEvent extends AbstractOnStoppingExecuteEvent {
    public KafkaSimpleListenerOnStoppingExecuteEvent(@NotNull final Object source) {
        super(source);
    }
}