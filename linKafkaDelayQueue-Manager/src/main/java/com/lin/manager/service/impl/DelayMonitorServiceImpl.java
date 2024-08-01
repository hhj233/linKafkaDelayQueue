package com.lin.manager.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lin.manager.service.DelayMonitorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author linzj
 */
@Service
@Slf4j
public class DelayMonitorServiceImpl implements DelayMonitorService {
     private Cache<String, String> cache =  Caffeine.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(24,TimeUnit.HOURS)
            .build();
    @Override
    public List delayMessageIntervalMonitor() {
        List<Long> delayInterval = new ArrayList<>();
        ConcurrentMap<@NonNull String, @NonNull String> map = cache.asMap();
        for (Map.Entry<String, String> value : map.entrySet()) {
            delayInterval.add(Long.parseLong(value.getValue()));
        }
        return delayInterval;
    }

    @KafkaListener(topics = "${spring.kafka.monitor-topic}", groupId = "${spring.kafka.monitor-topic-group-id}", containerFactory = "consumerFactory")
    public void delayMessageIntervalMonitorConsumer(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.info("monitor consumer:value:{}",record.value());
            String key = Objects.isNull(record.key()) ? UUID.randomUUID().toString() : record.key();
            String value = record.value();
            cache.put(key, value);
        }finally {
            acknowledgment.acknowledge();
        }
    }


}
