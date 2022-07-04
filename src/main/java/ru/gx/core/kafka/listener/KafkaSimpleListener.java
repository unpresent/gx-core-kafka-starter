package ru.gx.core.kafka.listener;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.util.CollectionUtils;
import ru.gx.core.channels.ChannelDirection;
import ru.gx.core.kafka.load.AbstractKafkaIncomeTopicsConfiguration;
import ru.gx.core.kafka.load.KafkaIncomeTopicsLoader;
import ru.gx.core.kafka.load.KafkaIncomeTopicsOffsetsController;
import ru.gx.core.kafka.offsets.TopicsOffsetsStorage;
import ru.gx.core.worker.AbstractWorker;

import java.util.List;

@Slf4j
public class KafkaSimpleListener extends AbstractWorker {
    public static final String SIMPLE_LISTENER_DEFAULT_NAME = "kafka-simple-listener";

    @NotNull
    private final String serviceName;

    @NotNull
    private final KafkaIncomeTopicsLoader kafkaIncomeTopicsLoader;

    @NotNull
    private final List<AbstractKafkaIncomeTopicsConfiguration> kafkaIncomeTopicsConfigurations;

    @NotNull
    private final TopicsOffsetsStorage topicsOffsetsStorage;

    @NotNull
    private final KafkaIncomeTopicsOffsetsController kafkaIncomeTopicsOffsetsController;

    public KafkaSimpleListener(
            @NotNull final String serviceName,
            @NotNull final String name,
            @NotNull final KafkaSimpleListenerSettingsContainer settingsContainer,
            @NotNull final MeterRegistry meterRegistry,
            @NotNull final ApplicationEventPublisher eventPublisher,
            @NotNull final KafkaIncomeTopicsLoader kafkaIncomeTopicsLoader,
            @NotNull final List<AbstractKafkaIncomeTopicsConfiguration> kafkaIncomeTopicsConfigurations,
            @NotNull final TopicsOffsetsStorage topicsOffsetsStorage,
            @NotNull final KafkaIncomeTopicsOffsetsController kafkaIncomeTopicsOffsetsController
    ) {
        super(name, settingsContainer, meterRegistry, eventPublisher);
        this.serviceName = serviceName;
        this.iterationExecuteEvent = new KafkaSimpleListenerOnIterationExecuteEvent(this);
        this.startingExecuteEvent = new KafkaSimpleListenerOnStartingExecuteEvent(this);
        this.stoppingExecuteEvent = new KafkaSimpleListenerOnStoppingExecuteEvent(this);
        this.kafkaIncomeTopicsLoader = kafkaIncomeTopicsLoader;
        this.kafkaIncomeTopicsConfigurations = kafkaIncomeTopicsConfigurations;
        this.topicsOffsetsStorage = topicsOffsetsStorage;
        this.kafkaIncomeTopicsOffsetsController = kafkaIncomeTopicsOffsetsController;
    }

    /**
     * Объект-команда, который является spring-event-ом. Его обработчик по сути должен содержать логику итераций
     */
    @Getter
    @NotNull
    private final KafkaSimpleListenerOnIterationExecuteEvent iterationExecuteEvent;

    /**
     * Объект-команда, который является spring-event-ом. Его обработчик по сути будет вызван перед запуском Исполнителя.
     */
    @Getter
    @NotNull
    private final KafkaSimpleListenerOnStartingExecuteEvent startingExecuteEvent;

    /**
     * Объект-команда, который является spring-event-ом. Его обработчик по сути будет вызван после останова Исполнителя.
     */
    @Getter
    @NotNull
    private final KafkaSimpleListenerOnStoppingExecuteEvent stoppingExecuteEvent;

    @Override
    public void runnerIsLifeSet() {
        super.runnerIsLifeSet();
    }

    @SuppressWarnings("unused")
    @EventListener(DoStartKafkaSimpleListenerEvent.class)
    public void doStartKafkaSimpleListenerEvent(DoStartKafkaSimpleListenerEvent __) {
        this.start();
    }

    @SuppressWarnings("unused")
    @EventListener(DoStopKafkaSimpleListenerEvent.class)
    public void doStopKafkaSimpleListenerEvent(DoStopKafkaSimpleListenerEvent __) {
        this.stop();
    }

    @Override
    protected KafkaSimpleListenerStatisticsInfo createStatisticsInfo() {
        return new KafkaSimpleListenerStatisticsInfo(this, this.getMeterRegistry());
    }

    @SuppressWarnings("unused")
    @EventListener(KafkaSimpleListenerOnStartingExecuteEvent.class)
    public void startKafkaListener(KafkaSimpleListenerOnStartingExecuteEvent __) {
        log.debug("Starting startKafkaListener()");

        for (final var config : this.kafkaIncomeTopicsConfigurations) {
            final var topicPartitionOffsets =
                    this.topicsOffsetsStorage.loadOffsets(ChannelDirection.In, this.serviceName, config);
            if (CollectionUtils.isEmpty(topicPartitionOffsets)) {
                this.kafkaIncomeTopicsOffsetsController.seekAllToBegin(config);
            } else {
                this.kafkaIncomeTopicsOffsetsController.seekTopicsByList(config, topicPartitionOffsets);
            }
        }

        log.debug("Finished startKafkaListener()");
    }

    @EventListener(KafkaSimpleListenerOnIterationExecuteEvent.class)
    public void executeIteration(KafkaSimpleListenerOnIterationExecuteEvent event) {
        log.debug("Starting executeIteration()");
        try {
            event.setImmediateRunNextIteration(false);

            for (final var config : this.kafkaIncomeTopicsConfigurations) {
                runnerIsLifeSet();
                final var loadedMessagesByTopic =
                        kafkaIncomeTopicsLoader.processAllTopics(config);

                if (log.isDebugEnabled()) {
                    for (final var entry : loadedMessagesByTopic.entrySet()) {
                        if (entry.getValue() > 1) {
                            log.debug("Loaded from {} {} records", entry.getKey().getChannelName(), entry.getValue());
                            event.setImmediateRunNextIteration(true);
                        }
                    }
                }
            }

        } catch (Exception e) {
            treatException(event, e);
        } finally {
            log.debug("Finished executeIteration()");
        }
    }

    @SuppressWarnings("unused")
    @EventListener(KafkaSimpleListenerOnStoppingExecuteEvent.class)
    public void stopKafkaListener(KafkaSimpleListenerOnStoppingExecuteEvent __) {
        log.debug("Starting stopKafkaListener()");
        log.debug("Finished stopKafkaListener()");
    }

    private void treatException(KafkaSimpleListenerOnIterationExecuteEvent event, Exception e) {
        log.error("", e);
        if (e instanceof InterruptedException) {
            log.info("event.setStopExecution(true)");
            event.setStopExecution(true);
        } else {
            log.info("event.setNeedRestart(true)");
            event.setNeedRestart(true);
        }
    }
}
