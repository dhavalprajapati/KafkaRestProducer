package org.icc.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Properties;
import java.util.concurrent.Future;

public class Writer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

    private final Properties myProducerProp;
    private final String myKafkaHost;
    private final String myTopic;
    private final KafkaProducer producer;

    public Writer(String topic, String kafkaHost) {
        myKafkaHost = kafkaHost;
        myTopic = topic;

        myProducerProp = new Properties();
        myProducerProp.put("metadata.broker.list", myKafkaHost);
        myProducerProp.put("bootstrap.servers", myKafkaHost);
        myProducerProp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        myProducerProp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        myProducerProp.put("reconnect.backoff.ms", "1000");
        myProducerProp.put("connect.timeout.ms", "3000");
        myProducerProp.put("producer.type", "async");
        myProducerProp.put("request.required.acks", "1");
        myProducerProp.put("metadata.fetch.timeout.ms", "3000");

        producer = new KafkaProducer(myProducerProp);
    }

    public Future send(String message) throws Exception{
        ProducerRecord record = new ProducerRecord(myTopic, message);
        return producer.send(record);
    }
}
