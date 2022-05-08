package ru.gx.core.kafka.listener;

import org.jetbrains.annotations.NotNull;
import ru.gx.core.worker.AbstractOnStartingExecuteEvent;

public class KafkaSimpleListenerOnStartingExecuteEvent extends AbstractOnStartingExecuteEvent {
    public KafkaSimpleListenerOnStartingExecuteEvent(@NotNull final Object source) {
        super(source);
    }
}
