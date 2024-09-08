package com.lin.manager.vo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author linzj
 */
@Data
public class DelayTopicDetailRespVo {
    /**
     * 主题
     */
    private String topic;

    /**
     * 分区
     */
    private List<TopicPartitionInfo> partitionInfo;

    /**
     * 大小
     */
    private Long size;

    /**
     * 消费者组
     */
    private String groupId;


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class TopicPartitionInfo {
        /**
         * 分区名
         */
        private Integer partition;
        /**
         * 领导者节点
         */
        private Node leader;
        /**
         * 追随者节点
         */
        private List<Node> replicas;
        /**
         * 同步节点
         */
        private List<Node> isr;

        /**
         * lag
         */
        private Long lag;

        /**
         * lead
         */
        private Long lead;


    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class Node {
        private  int id;
        private String idString;
        private String host;
        private int port;
        private String rack;
    }
}
