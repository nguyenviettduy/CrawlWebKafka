package com.kafka.crawl.configuration;

import com.kafka.crawl.utils.Constant;
import com.kafka.crawl.utils.LoadProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;

public class Configuration {
    public static Logger logger = LoggerFactory.getLogger(Configuration.class);

    public static Consumer<String, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LoadProperties.getInstance().getProperties().getProperty(Constant.KAFKA_BOOTSTRAP_SERVER));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupid");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(LoadProperties.getInstance().getProperties().getProperty(Constant.KAFKA_TOPIC)));
        return consumer;
    }

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, LoadProperties.getInstance().getProperties().getProperty(Constant.KAFKA_BOOTSTRAP_SERVER));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
