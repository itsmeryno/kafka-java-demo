package com.itsmeryno.kafka.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class KafkaDemoApplication {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class);

//        KafkaDemoApplication app = new KafkaDemoApplication();
//
//        try {
//            app.createTopic("kafka-test-topic");
//            app.sendMessage("kafka-test-topic");
//        }
//        catch (Exception ex) {
//            ex.printStackTrace();
//            LOG.error("Unable to produce message", ex);
//        }
    }

    private void sendMessage(String topicName) throws ExecutionException, InterruptedException {
        KafkaProducer<Long, String> producer = createProducer();
        ProducerRecord<Long, String> record = new ProducerRecord<>(topicName, 1L, "Hello world");
        RecordMetadata metadata = producer.send(record).get();
        LOG.info("Message sent: {}", metadata);
    }

    private void createTopic(String topicName) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);
        NewTopic newTopic = new NewTopic(topicName, 1, (short)1);
        CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
        LOG.info("created topic: {}", topicName);
    }

    private KafkaProducer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<Long, String>(props);
    }
}
