package com.lin.manager.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

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
}
