package com.lin.core.runner;

import com.lin.common.util.JsonUtil;
import com.lin.common.message.DelayMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author linzj
 */
@Slf4j
public class DelayMessageRunner implements Runnable {
    private final KafkaConsumer<String,String> consumer;
    private final KafkaProducer<String,String> producer;
    private final Object lock = new Object();
    private final String topic;
    private final Long delayTime;
    private final Timer timer = new Timer();
    private volatile boolean running = true;

    public DelayMessageRunner(String servers, String groupId, String topic, Long delayTime) {
        this.consumer = createKafkaConsumer(servers,groupId);
        this.producer = createKafkaProducer(servers);
        this.topic = topic;
        this.delayTime = delayTime;

        this.consumer.subscribe(Collections.singleton(topic));

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                synchronized (lock) {
                    consumer.resume(consumer.paused());
                    lock.notify();
                }
            }
        }, 0,200);
    }

    @Override
    public void run() {
        try{
            delayQueueCoreHandler();
        }finally {
            this.consumer.close();
            this.producer.close();
            log.debug("close interval topic consumer and producer");
        }
    }

    @SneakyThrows
    private void delayQueueCoreHandler() {
        do {
            synchronized (lock) {
                ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofMillis(200));
                if (consumerRecords.isEmpty()) {
                    lock.wait();
                    continue;
                }
                log.info("pull {} messages from {}", consumerRecords.count(), topic);
                boolean timed = false;
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    long timestamp = consumerRecord.timestamp();
                    TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                    if (timestamp + delayTime < System.currentTimeMillis()) {
                        String value = consumerRecord.value();
                        DelayMessage delayMessage;
                        try {
                            delayMessage = JsonUtil.parse(value, DelayMessage.class);
                        } catch (Exception e) {
                            log.warn("delayMessage parse json error: {}", e.getMessage());
                            continue;
                        }
                        String targetTopic = delayMessage.getTopic();
                        String targetKey = delayMessage.getKey();
                        String targetValue = delayMessage.getValue();
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, targetKey, targetValue);
                        try {
                            RecordMetadata recordMetadata = this.producer.send(producerRecord).get();
                            log.info("send delay message to targetUser, topic:{}, key:{}, value:{}, offset:{}",
                                    targetTopic, targetKey, targetValue, recordMetadata.offset());
                            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(recordMetadata.offset() + 1);
                            Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
                            metadataMap.put(topicPartition, offsetAndMetadata);
                            consumer.commitSync(metadataMap);
                        } catch (ExecutionException e) {
                            consumer.pause(Collections.singletonList(topicPartition));
                            consumer.seek(topicPartition, consumerRecord.offset());
                            timed = true;
                            break;

                        }
                    } else if (timestamp + delayTime - 800 < System.currentTimeMillis()) {
                        log.info("enter 800ms message delay interval");
                        long delayTime = timestamp + this.delayTime - System.currentTimeMillis();
                        try {
                            Thread.sleep(delayTime);
                        }catch (InterruptedException e) {
                            log.error("thread sleep interrupted error",e);
                            consumer.pause(Collections.singletonList(topicPartition));
                            consumer.seek(topicPartition, consumerRecord.offset());
                            timed = true;
                            break;
                        }
                        String value = consumerRecord.value();
                        DelayMessage delayMessage;
                        try {
                            delayMessage = JsonUtil.parse(value, DelayMessage.class);
                        } catch (Exception e) {
                            log.warn("delayMessage parse json error: {}", e.getMessage());
                            continue;
                        }
                        String targetTopic = delayMessage.getTopic();
                        String targetKey = delayMessage.getKey();
                        String targetValue = delayMessage.getValue();
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, targetKey, targetValue);
                        try {
                            RecordMetadata recordMetadata = this.producer.send(producerRecord).get();
                            log.info("send delay message to targetUser, topic:{}, key:{}, value:{}, offset:{}",
                                    targetTopic, targetKey, targetValue, recordMetadata.offset());
                            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(recordMetadata.offset() + 1);
                            Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
                            metadataMap.put(topicPartition, offsetAndMetadata);
                            consumer.commitSync(metadataMap);
                        } catch (ExecutionException e) {
                            consumer.pause(Collections.singletonList(topicPartition));
                            consumer.seek(topicPartition, consumerRecord.offset());
                            timed = true;
                            break;

                        }
                    }
                    else {
                        consumer.pause(Collections.singletonList(topicPartition));
                        consumer.seek(topicPartition, consumerRecord.offset());
                        timed = true;
                        break;
                    }
                }
                if (timed) {
                    lock.wait();
                }
            }
        } while (this.running);
    }

    public void shutdown() {
        this.timer.cancel();
        this.running = false;
        synchronized (lock) {
            lock.notify();
        }
    }
    private KafkaConsumer<String,String> createKafkaConsumer(String servers, String groupId) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        prop.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<String, String>(prop);
    }

    private KafkaProducer<String,String> createKafkaProducer(String servers) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.RETRIES_CONFIG, "3");
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(prop);
    }
}
