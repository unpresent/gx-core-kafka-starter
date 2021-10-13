package ru.gx.kafka.load;

/**
 * Ошибки при загрузке и обработке входящих сообщений.
 */
public class IncomeTopicsLoadingException extends RuntimeException {
    public IncomeTopicsLoadingException(String message) {
        super(message);
    }
}
