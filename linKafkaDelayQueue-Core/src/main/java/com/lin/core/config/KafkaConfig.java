package com.lin.core.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.util.Map;

/**
 * @author linzj
 */
@Configuration
@ConfigurationProperties(prefix = "spring.kafka")
@Data
@Slf4j
public class KafkaConfig {
    private final static String BATCH_SIZE = "batch-size";
    private final static String BUFFER_MEMORY = "buffer-memory";
    private final static String AUTO_OFFSET_RESET = "auto-offset-reset";
    private final static String ENABLE_AUTO_COMMIT = "enable-auto-commit";
    private final static String SESSION_TIMEOUT_MS = "session-timeout-ms";
    private final static String HEARTBEAT_INTERVAL = "heartbeat-interval";
    private String bootstrapServers;
    /**
     * 生产者配置
     */
    private Map<String, Object> producer;
    /**
     * 消费者配置
     */
    private Map<String, Object> consumer;

}
