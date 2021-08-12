package ru.gxfin.common.kafka.annotations;

@Deprecated
public interface IncomeTopicListenerRegistry {
    /**
     * Регистрирует в реестре бин с обновляемыми свойствами.
     * @param beanName   имя бина.
     * @param bean       бин.
     * @param proxyBean  запроксированный бин.
     * @param annotation аннотация.
     */
    void registerBean(String beanName, Object bean, Object proxyBean, IncomeTopicListener annotation);
}
