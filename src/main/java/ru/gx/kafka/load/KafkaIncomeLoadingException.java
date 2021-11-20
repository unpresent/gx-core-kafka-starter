package ru.gx.kafka.load;

/**
 * Ошибки при загрузке и обработке входящих сообщений.
 */
public class KafkaIncomeLoadingException extends RuntimeException {
    public KafkaIncomeLoadingException(String message) {
        super(message);
    }
}
