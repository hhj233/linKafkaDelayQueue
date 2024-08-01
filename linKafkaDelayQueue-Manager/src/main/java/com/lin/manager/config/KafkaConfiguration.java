package com.lin.manager.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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
}
