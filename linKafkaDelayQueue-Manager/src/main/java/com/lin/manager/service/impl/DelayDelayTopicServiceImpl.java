package com.lin.manager.service.impl;

import com.lin.common.util.ExceptionUtil;
import com.lin.manager.dto.DelayTopicDetailDto;
import com.lin.manager.service.DelayTopicService;
import com.lin.manager.vo.DelayTopicDetailRespVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author linzj
 */
@Service
@Slf4j
public class DelayDelayTopicServiceImpl implements DelayTopicService {
    private static final String DELAY_QUEUE_PREFIX = "kafka__delay";
    @Autowired
    private AdminClient adminClient;

    @Override
    public List<String> allDelayTopic() {
        List<String> topicList;
        try {
            Set<String> topicSet = adminClient.listTopics().names().get();
            if (topicSet.isEmpty()) {
                return null;
            }
            topicList = topicSet.stream().filter(i -> i.contains(DELAY_QUEUE_PREFIX)).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("query kafka topic error,{}", ExceptionUtil.getStackTraceAsString(e));
            return null;
        }
        return topicList;
    }


    @Override
    public DelayTopicDetailRespVo delayTopicDetail(DelayTopicDetailDto req) {
        DelayTopicDetailRespVo resp = new DelayTopicDetailRespVo();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singleton(req.getTopic()));
        try {
            Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
            TopicDescription topicDescription = topicDescriptionMap.get(req.getTopic());
            resp.setTopic(req.getTopic());
            List<DelayTopicDetailRespVo.TopicPartitionInfo> topicPartitionInfos = new ArrayList<>();
            for (TopicPartitionInfo partition : topicDescription.partitions()) {
                DelayTopicDetailRespVo.TopicPartitionInfo partitionInfo = DelayTopicDetailRespVo.TopicPartitionInfo.builder()
                        .partition(partition.partition())
                        .leader(DelayTopicDetailRespVo.Node.builder()
                                .id(partition.leader().id())
                                .host(partition.leader().host())
                                .port(partition.leader().port())
                                .rack(partition.leader().rack())
                                .idString(partition.leader().idString())
                                .build())
                        .replicas(partition.replicas().stream().map(i -> {
                            return DelayTopicDetailRespVo.Node.builder()
                                    .id(i.id())
                                    .host(i.host())
                                    .rack(i.rack())
                                    .port(i.port())
                                    .idString(i.idString())
                                    .build();
                        }).collect(Collectors.toList()))
                        .isr(partition.isr().stream().map(i -> {
                            return DelayTopicDetailRespVo.Node.builder()
                                    .id(i.id())
                                    .host(i.host())
                                    .rack(i.rack())
                                    .port(i.port())
                                    .idString(i.idString())
                                    .build();
                        }).collect(Collectors.toList()))
                        .build();
                topicPartitionInfos.add(partitionInfo);
            }
            resp.setPartitionInfo(topicPartitionInfos);
            // 获取topic消息大小
            resp.setSize(getTopicMessageSize(req.getTopic()));
        } catch (InterruptedException e) {
            log.error("get kafka topic tail error, {}", ExceptionUtil.getStackTraceAsString(e));
        } catch (ExecutionException e) {
            log.error("get kafka topic tail error, {}", ExceptionUtil.getStackTraceAsString(e));
        }

        return resp;
    }

    private Long getTopicMessageSize(String topic) throws ExecutionException, InterruptedException {
        Long topicSize = new Long(0L);
        // 获取所有节点
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        Collection<Node> nodes = describeClusterResult.nodes().get();
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> integerMapMap = adminClient.describeLogDirs(nodes.stream().map(Node::id).collect(Collectors.toList())).all().get();
        for (Map<String, DescribeLogDirsResponse.LogDirInfo> value : integerMapMap.values()) {
            for (DescribeLogDirsResponse.LogDirInfo logDirInfo : value.values()) {
                for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> topicPartitionReplicaInfoEntry : logDirInfo.replicaInfos.entrySet()) {
                    TopicPartition key = topicPartitionReplicaInfoEntry.getKey();
                    if (key.topic().contains(topic)) {
                        DescribeLogDirsResponse.ReplicaInfo replicaInfo = topicPartitionReplicaInfoEntry.getValue();
                        topicSize = topicSize + replicaInfo.size;
                    }
                }
            }
        }
        return topicSize;
    }
}
