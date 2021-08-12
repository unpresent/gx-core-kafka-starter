package ru.gxfin.common.kafka.annotations;

import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.AliasFor;
import ru.gxfin.common.kafka.events.OnObjectsLoadedFromIncomeTopicEvent;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@EventListener
@Deprecated
public @interface IncomeTopicListener {
    @AliasFor("classes")
    Class<? extends OnObjectsLoadedFromIncomeTopicEvent>[] value() default {};

    @SuppressWarnings("unused")
    @AliasFor("value")
    Class<? extends OnObjectsLoadedFromIncomeTopicEvent>[] classes() default {};
}
