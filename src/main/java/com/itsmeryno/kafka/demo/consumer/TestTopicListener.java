package com.itsmeryno.kafka.demo.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TestTopicListener {
    private static final Logger LOG = LoggerFactory.getLogger(TestTopicListener.class);

    @KafkaListener(topics = "kafka-test-topic", groupId = "foo")
    public void listen(String message) {
        LOG.info("message received: {}", message);
    }

}
