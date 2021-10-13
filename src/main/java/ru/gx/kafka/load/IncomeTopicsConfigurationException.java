package ru.gx.kafka.load;

/**
 * Ошибки в конфигурации описателей входящих очередей.
 */
public class IncomeTopicsConfigurationException extends RuntimeException {
    public IncomeTopicsConfigurationException(String message) {
        super(message);
    }
}
