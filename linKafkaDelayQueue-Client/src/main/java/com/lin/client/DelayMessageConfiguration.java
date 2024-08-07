package com.lin.client;

import com.lin.client.delay.DelayMessageQueue;
import com.lin.common.config.DelayMessageConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * @author Administrator
 */
@Configuration
public class DelayMessageConfiguration {

    @Bean(value = "delayMessageConfig")
    @ConfigurationProperties(prefix = "delay-message")
    public DelayMessageConfig delayMessageConfig() {
        return new DelayMessageConfig();
    }

    @Bean(value = "delayMessageQueue", initMethod = "init", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(value = DelayMessageQueue.class)
    public DelayMessageQueue delayMessageQueue(DelayMessageConfig config, AdminClient client) {
        return new DelayMessageQueue(config, client);
    }

    @Bean(value = "adminClient", destroyMethod = "close")
    @ConditionalOnMissingBean(value = AdminClient.class)
    public AdminClient adminClient(DelayMessageConfig config) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getServers());
        AdminClient adminClient = KafkaAdminClient.create(prop);
        return adminClient;
    }
}
