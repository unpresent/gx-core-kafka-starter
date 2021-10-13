package ru.gx.kafka.upload;

/**
 * Ошибки в конфигурации описателей исходящих очередей.
 */
public class OutcomeTopicsConfigurationException extends RuntimeException {
    public OutcomeTopicsConfigurationException(String message) {
        super(message);
    }
}
