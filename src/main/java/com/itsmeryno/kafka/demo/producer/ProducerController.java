package com.itsmeryno.kafka.demo.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {
    private final KafkaTemplate<Long, String> kafkaTemplate;

    @Autowired
    ProducerController(KafkaTemplate<Long, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/send")
    public void sendMessage() {
        kafkaTemplate.send("kafka-test-topic", "Hello world");
    }

}
