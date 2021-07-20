package ru.gxfin.common.kafka;

/**
 * Варианты управляющего сигнала из обработчика события о чтении топика загрузчику топиков.
 */
@SuppressWarnings("unused")
public enum IncomeTopicsLoaderContinueMode {
    /**
     * Автоматический выбор между {@link #NextTopic} и {@link #NextIteration}.
     */
    Auto,

    /**
     * Перейти к следующему Topic-у.
     */
    NextTopic,

    /**
     * Перейти к следующей Итерации, пропустив все топики более низкого приоритета.
     */
    NextIteration
}
