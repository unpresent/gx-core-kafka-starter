package ru.gx.core.kafka.listener;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import ru.gx.core.settings.StandardSettingsController;
import ru.gx.core.worker.AbstractWorkerSettingsContainer;
import ru.gx.core.worker.CommonWorkerSettingsDefaults;

import javax.annotation.PostConstruct;

import static lombok.AccessLevel.PROTECTED;

@SuppressWarnings("unused")
public class KafkaSimpleListenerSettingsContainer extends AbstractWorkerSettingsContainer {
    public final static String SIMPLE_LISTENER_SETTINGS_PREFIX = "service.kafka.simple-listener";
    private final static String SETTING_WAIT_ON_STOP_MS = SIMPLE_LISTENER_SETTINGS_PREFIX + "." + AbstractWorkerSettingsContainer.WAIT_ON_STOP_MS;
    private final static String SETTING_WAIT_ON_RESTART_MS = SIMPLE_LISTENER_SETTINGS_PREFIX + "." + AbstractWorkerSettingsContainer.WAIT_ON_RESTART_MS;
    private final static String SETTING_MIN_TIME_PER_ITERATION_MS = SIMPLE_LISTENER_SETTINGS_PREFIX + "." + AbstractWorkerSettingsContainer.MIN_TIME_PER_ITERATION_MS;
    private final static String SETTING_TIMOUT_RUNNER_LIFE_MS = SIMPLE_LISTENER_SETTINGS_PREFIX + "." + AbstractWorkerSettingsContainer.TIMEOUT_RUNNER_LIFE_MS;
    private final static String SETTING_PRINT_STATISTICS_EVERY_MS = SIMPLE_LISTENER_SETTINGS_PREFIX + "." + AbstractWorkerSettingsContainer.PRINT_STATISTICS_EVERY_MS;

    @Getter(PROTECTED)
    @NotNull
    private final StandardSettingsController standardSettingsController;

    public KafkaSimpleListenerSettingsContainer(@NotNull final StandardSettingsController standardSettingsController) {
        this.standardSettingsController = standardSettingsController;
    }

    @PostConstruct
    public void init() {
        this.standardSettingsController.loadIntegerSetting(SETTING_WAIT_ON_STOP_MS, CommonWorkerSettingsDefaults.WAIT_ON_STOP_MS_DEFAULT);
        this.standardSettingsController.loadIntegerSetting(SETTING_WAIT_ON_RESTART_MS, CommonWorkerSettingsDefaults.WAIT_ON_RESTART_MS_DEFAULT);
        this.standardSettingsController.loadIntegerSetting(SETTING_MIN_TIME_PER_ITERATION_MS, CommonWorkerSettingsDefaults.MIN_TIME_PER_ITERATION_MS_DEFAULT);
        this.standardSettingsController.loadIntegerSetting(SETTING_TIMOUT_RUNNER_LIFE_MS, CommonWorkerSettingsDefaults.TIMEOUT_RUNNER_LIFE_MS_DEFAULT);
        this.standardSettingsController.loadIntegerSetting(SETTING_PRINT_STATISTICS_EVERY_MS, CommonWorkerSettingsDefaults.PRINT_STATISTICS_EVERY_MS_DEFAULT);
    }

    @Override
    public int getWaitOnStopMs() {
        return this.standardSettingsController.getIntegerSetting(SETTING_WAIT_ON_STOP_MS);
    }

    @Override
    public int getWaitOnRestartMs() {
        return this.standardSettingsController.getIntegerSetting(SETTING_WAIT_ON_RESTART_MS);
    }

    @Override
    public int getMinTimePerIterationMs() {
        return this.standardSettingsController.getIntegerSetting(SETTING_MIN_TIME_PER_ITERATION_MS);
    }

    @Override
    public int getTimeoutRunnerLifeMs() {
        return this.standardSettingsController.getIntegerSetting(SETTING_TIMOUT_RUNNER_LIFE_MS);
    }

    @Override
    public int getPrintStatisticsEveryMs() {
        return this.standardSettingsController.getIntegerSetting(SETTING_PRINT_STATISTICS_EVERY_MS);
    }
}
