package ru.gx.kafka.events;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationEvent;
import ru.gx.kafka.load.*;

import java.util.Collection;

import static lombok.AccessLevel.PROTECTED;

@Getter
@Setter(PROTECTED)
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@ToString
public class AbstractOnRawDataLoadedFromIncomeTopicEvent
        extends ApplicationEvent
        implements OnRawDataLoadedFromIncomeTopicEvent {

    /**
     * Описатель загрузки из Топика.
     */
    @NotNull
    private RawDataIncomeTopicLoadingDescriptor loadingDescriptor;

    /**
     * Загруженные из Kafka данные.
     */
    @NotNull
    private Collection<ConsumerRecord<?, ?>> data;

    /**
     * Режим продолжения обработки других Топиков.
     */
    @NotNull
    private IncomeTopicsLoaderContinueMode continueMode;

    protected AbstractOnRawDataLoadedFromIncomeTopicEvent(Object source) {
        super(source);
        this.continueMode = IncomeTopicsLoaderContinueMode.Auto;
    }

    @Override
    public @NotNull OnRawDataLoadedFromIncomeTopicEvent reset(@NotNull final Object source, @NotNull final RawDataIncomeTopicLoadingDescriptor loadingDescriptor, @NotNull final IncomeTopicsLoaderContinueMode continueMode, @NotNull final Collection<ConsumerRecord<?, ?>> records) {
        super.source = source;
        return this
                .setLoadingDescriptor(loadingDescriptor)
                .setContinueMode(continueMode)
                .setData(records);
    }
}
