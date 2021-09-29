package ru.gx.kafka.uploader;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.producer.Producer;
import ru.gx.kafka.TopicMessageMode;

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
