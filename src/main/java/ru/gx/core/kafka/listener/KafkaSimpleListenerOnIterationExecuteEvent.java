package ru.gx.core.kafka.listener;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.worker.AbstractOnIterationExecuteEvent;

public class KafkaSimpleListenerOnIterationExecuteEvent extends AbstractOnIterationExecuteEvent {
    public KafkaSimpleListenerOnIterationExecuteEvent(@NotNull final Object source) {
        super(source);
    }
}
