package com.lin.client;

import com.lin.client.delay.DelayMessageQueue;
import com.lin.common.config.DelayMessageConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    public DelayMessageQueue delayMessageQueue(DelayMessageConfig config) {
        return new DelayMessageQueue(config);
    }
}
