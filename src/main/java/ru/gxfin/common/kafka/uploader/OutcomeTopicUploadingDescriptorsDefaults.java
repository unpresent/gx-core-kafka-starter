package ru.gxfin.common.kafka.uploader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.Producer;
import ru.gxfin.common.kafka.TopicMessageMode;
import ru.gxfin.common.kafka.loader.LoadingFiltering;
import ru.gxfin.common.kafka.loader.LoadingMode;

import java.util.Properties;

@Getter
@Setter
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = false)
@ToString
public class OutcomeTopicUploadingDescriptorsDefaults {
    private TopicMessageMode topicMessageMode = TopicMessageMode.OBJECT;

    private Producer<Long, String> producer;

    public OutcomeTopicUploadingDescriptorsDefaults() {
        super();
    }
}
