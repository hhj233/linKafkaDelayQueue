package com.lin.manager.config;

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
    public final static String BATCH_SIZE = "batch-size";
    public final static String BUFFER_MEMORY = "buffer-memory";
    public final static String AUTO_OFFSET_RESET = "auto-offset-reset";
    public final static String ENABLE_AUTO_COMMIT = "enable-auto-commit";
    public final static String SESSION_TIMEOUT_MS = "session-timeout-ms";
    public final static String HEARTBEAT_INTERVAL = "heartbeat-interval";
    public final static String GROUP_ID = "group-id";
    public String bootstrapServers;
    /**
     * 生产者配置
     */
    private Map<String, Object> producer;
    /**
     * 消费者配置
     */
    private Map<String, Object> consumer;

}
