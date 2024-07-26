package com.lin.common.config;

import lombok.Data;

/**
 * @author linzj
 */
@Data
public class DelayMessageConfig {
    /**
     * kafka集群地址
     */
    private String servers;
    /**
     * 延时队列topic
     */
    private String topic;
    /**
     * 延时消费组
     */
    private String groupId;
}
