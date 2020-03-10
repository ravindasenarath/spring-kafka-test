package com.ravinda.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.stereotype.Component;

@Component
public class MyTestListner {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MyTestListner.class);

    private final KafkaOperations<String, String> publisher;

    public MyTestListner(KafkaOperations<String, String> publisher) {
        this.publisher = publisher;
    }

    @KafkaListener(
            id = "scheduling",
            topics = "my-test-topic-upstream",
            containerFactory = "schedulingListenerFactory",
            groupId = "${kafka.application.groupId}")
    public void listen(ConsumerRecord<String, String> record){
        LOGGER.info("key - {}, value - {}", record.key(), record.value());
        publisher.send("my-test-topic-downstream", record.key(), record.value());
    }

}
