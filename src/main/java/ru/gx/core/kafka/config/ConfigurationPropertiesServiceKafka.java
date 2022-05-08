package ru.gx.core.kafka.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import ru.gx.core.config.ConfigurationPropertiesService;
import ru.gx.core.worker.CommonWorkerSettingsDefaults;

@ConfigurationProperties(prefix = "service.kafka")
@Getter
@Setter
public class ConfigurationPropertiesServiceKafka {
    private String server;

    @NestedConfigurationProperty
    private IncomeTopics incomeTopics;

    @NestedConfigurationProperty
    private OutcomeTopics outcomeTopics;

    @NestedConfigurationProperty
    private SimpleListener simpleListener = new SimpleListener();

    @Getter
    @Setter
    public static class IncomeTopics {
        @NestedConfigurationProperty
        private StandardLoader standardLoader = new StandardLoader();

        @NestedConfigurationProperty
        private StandardOffsetsController standardOffsetsController = new StandardOffsetsController();
    }

    @Getter
    @Setter
    public static class OutcomeTopics {
        @NestedConfigurationProperty
        private StandardUploader standardUploader = new StandardUploader();
    }

    @Getter
    @Setter
    public static class StandardOffsetsController {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class StandardLoader {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class StandardUploader {
        private boolean enabled = true;
    }

    @Getter
    @Setter
    public static class SimpleListener {
        public static final String NAME_DEFAULT = "simple-listener";

        private boolean enabled = false;
        private String name = NAME_DEFAULT;
        private int waitOnStopMs = CommonWorkerSettingsDefaults.WAIT_ON_STOP_MS_DEFAULT;
        private int waitOnRestartMs = CommonWorkerSettingsDefaults.WAIT_ON_RESTART_MS_DEFAULT;
        private int minTimePerIterationMs = CommonWorkerSettingsDefaults.MIN_TIME_PER_ITERATION_MS_DEFAULT;
        private int timeoutRunnerLifeMs = CommonWorkerSettingsDefaults.TIMEOUT_RUNNER_LIFE_MS_DEFAULT;
        private int printStatisticsEveryMs = CommonWorkerSettingsDefaults.PRINT_STATISTICS_EVERY_MS_DEFAULT;
    }

}
