package com.lin.manager.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.util.Assert;

import java.util.*;

import static com.lin.manager.config.KafkaConfig.*;


/**
 * @author linzj
 */
@Configuration
public class KafkaConfiguration {

    @Bean(value = "adminClient")
    @ConditionalOnMissingBean(value = AdminClient.class)
    public AdminClient adminClient(KafkaConfig kafkaConfig) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        AdminClient adminClient = KafkaAdminClient.create(prop);
        return adminClient;
    }

    @Bean(value = "consumerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> consumerFactory(KafkaConfig kafkaConfig) {
        ConcurrentKafkaListenerContainerFactory consumerFactory = new ConcurrentKafkaListenerContainerFactory();
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.getConsumer().get(AUTO_OFFSET_RESET));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConfig.getConsumer().get(ENABLE_AUTO_COMMIT));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaConfig.getConsumer().get(SESSION_TIMEOUT_MS));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafkaConfig.getConsumer().get(HEARTBEAT_INTERVAL));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        consumerFactory.setConsumerFactory(new DefaultKafkaConsumerFactory(props));
        consumerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return consumerFactory;
    }

    @Bean(value = "producerFactory")
    public ProducerFactory<String, String> producerFactory(KafkaConfig kafkaConfig) {
        Assert.notNull(kafkaConfig.getBootstrapServers(), "kafka broker address not configured");
        Assert.notNull(kafkaConfig.getProducer(), "kafka producer config not configured");
        Assert.notNull(kafkaConfig.getProducer().get(ProducerConfig.ACKS_CONFIG), "kafka producer.acks not configured");
        Assert.notNull(kafkaConfig.getProducer().get(ProducerConfig.RETRIES_CONFIG), "kafka producer.reties not configured");
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.getProducer().get(ProducerConfig.ACKS_CONFIG));
        props.put(ProducerConfig.RETRIES_CONFIG, kafkaConfig.getProducer().get(ProducerConfig.RETRIES_CONFIG));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,kafkaConfig.getProducer().get(BATCH_SIZE));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,kafkaConfig.getProducer().get(BUFFER_MEMORY));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        List<Class> kafkaProducerInterceptorList = new ArrayList<>();
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, kafkaProducerInterceptorList);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(value = "kafkaTemplate")
    @ConditionalOnMissingBean(value = KafkaTemplate.class)
    public KafkaTemplate<String,String> kafkaTemplate(ProducerFactory factory) {
        return new KafkaTemplate<>(factory);
    }
}
