package ru.gx.core.kafka.listener;

import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

/**
 * Объект-событие.<br/>
 * Публикация данного события запускает Worker.
 * Слушателем данного является сам Worker.
 */
@SuppressWarnings("unused")
public class DoStartKafkaSimpleListenerEvent extends ApplicationEvent {
    protected DoStartKafkaSimpleListenerEvent(@NotNull final Object source) {
        super(source);
    }

    public static void publish(@NotNull final ApplicationEventPublisher publisher, @NotNull final Object source) {
        final var event = new DoStartKafkaSimpleListenerEvent(source);
        publisher.publishEvent(event);
    }

    public static void publish(@NotNull final ApplicationContext context, @NotNull final Object source) {
        final var event = new DoStartKafkaSimpleListenerEvent(source);
        context.publishEvent(event);
    }
}
