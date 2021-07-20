package ru.gxfin.common.kafka.annotations;

import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.AliasFor;
import ru.gxfin.common.kafka.events.ObjectsLoadedFromIncomeTopicEvent;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@SuppressWarnings("rawtypes")
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@EventListener
public @interface IncomeTopicListener {
    @AliasFor("classes")
    Class<ObjectsLoadedFromIncomeTopicEvent>[] value() default {};

    @AliasFor("value")
    Class<ObjectsLoadedFromIncomeTopicEvent>[] classes() default {};
}
