package com.lin.core.runner;

import com.lin.common.util.JsonUtil;
import com.lin.common.message.DelayMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author linzj
 */
@Slf4j
public class DelayMessageRunner implements Runnable {
    private final KafkaConsumer<String,String> consumer;
    private final KafkaProducer<String,String> producer;
    private final Object lock = new Object();
    private final String monitorTopic;
    private final String topic;
    private final Long delayTime;
    private final Timer timer;
    private final ExecutorService delayMessageForwardFailTimer;
    private volatile boolean running = true;
    private TopicPartition topicPartition;
    private volatile AtomicLong successMaxOffset;
    private Long prevCommitOffset;
    private LinkedBlockingDeque<DelayMessage> delayMessageForwardFailQueue;

    public DelayMessageRunner(String servers, String groupId, String monitorTopic, String topic, Long delayTime) {
        this.consumer = createKafkaConsumer(servers,groupId);
        this.producer = createKafkaProducer(servers);
        this.monitorTopic = monitorTopic;
        this.topic = topic;
        this.delayTime = delayTime;
        this.successMaxOffset = new AtomicLong(0L);
        this.prevCommitOffset = 0L;
        this.delayMessageForwardFailQueue = new LinkedBlockingDeque<>();
        this.timer = new Timer("delay-Timer");
        this.delayMessageForwardFailTimer = new ThreadPoolExecutor(1,1,0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1));
        this.consumer.subscribe(Collections.singleton(topic));

    }
    private void initDelaySchedule() {
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                synchronized (lock) {
                    consumer.resume(consumer.paused());
                    lock.notify();
                }
            }
        }, 0,200);
        this.delayMessageForwardFailTimer.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        DelayMessage delayMessage = delayMessageForwardFailQueue.take();
                        delayMessageForwardFailRetryHandler(delayMessage);
                    } catch (InterruptedException e) {
                        log.error("delay message forward fail queue take error:{}", e.getMessage());
                    }
                }
            }
        });

    }

    @Override
    public void run() {
        initDelaySchedule();
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
                consumerCommitMaxOffset();
                ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofMillis(200));
                if (consumerRecords.isEmpty()) {
                    lock.wait();
                    continue;
                }
                log.info("pull {} messages from {}", consumerRecords.count(), topic);
                boolean timed = false;
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("delay queue topic:{},partition:{},offset:{}",consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                    long timestamp = consumerRecord.timestamp();
                    TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                    this.topicPartition = topicPartition;
                    if (timestamp + this.delayTime + 1000 * 60 * 60 * 2 < System.currentTimeMillis()) {
                        log.warn("delay message is more than 2 hours old");
                        continue;
                    }
                    else if (timestamp + this.delayTime < System.currentTimeMillis()) {
                        if (!delayMessageForwardHandler(consumerRecord)) {
                            timed = true;
                            break;
                        }
                    } else if (timestamp + this.delayTime - 800 < System.currentTimeMillis()) {
                        log.info("enter 800ms message delay interval");
                        long delayWaitTime = timestamp + this.delayTime - System.currentTimeMillis();
                        try {
                            if(delayWaitTime > 0) {
                                Thread.sleep(delayWaitTime);
                            }
                        }catch (InterruptedException e) {
                            log.error("thread sleep interrupted error",e);
                            consumer.pause(Collections.singletonList(topicPartition));
                            consumer.seek(topicPartition, consumerRecord.offset());
                            timed = true;
                            break;
                        }
                        if (!delayMessageForwardHandler(consumerRecord)) {
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

    private Boolean delayMessageForwardHandler(ConsumerRecord<String,String> consumerRecord) {
        long timestamp = consumerRecord.timestamp();
        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        String value = consumerRecord.value();
        DelayMessage delayMessage;
        try {
            delayMessage = JsonUtil.parse(value, DelayMessage.class);
        } catch (Exception e) {
            log.warn("delayMessage parse json error: {}", e.getMessage());
            return true;
        }
        String targetTopic = delayMessage.getTopic();
        String targetKey = delayMessage.getKey();
        String targetValue = delayMessage.getValue();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, targetKey, targetValue);
        try {
            long delayTime = this.delayTime;
            this.producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (Objects.nonNull(recordMetadata)) {
                        log.info("send delay message to targetUser, topic:{}, key:{}, value:{}, offset:{}",
                                targetTopic, targetKey, targetValue, recordMetadata.offset());
                        long currentSuccessOffset;
                        long successWaitOffset = consumerRecord.offset() + 1;
                        do {
                            currentSuccessOffset = successMaxOffset.get();
                            successWaitOffset = Math.max(successWaitOffset, currentSuccessOffset);
                        }while (!successMaxOffset.compareAndSet(currentSuccessOffset, successWaitOffset));
                    } else {
                        // todo send delay message error logic
                        log.error("send delay message to targetUser error, topic:{}, key:{}, value:{}, reason:{}",
                                targetTopic, targetKey, targetValue,Objects.nonNull(e) ? e.getMessage() : "");
                        try {
                            delayMessageForwardFailQueue.put(delayMessage);
                        } catch (InterruptedException ex) {
                            log.error("delay message put retry queue error, {}", e.getMessage());
                        }
                    }

                }
            });
            monitorKafkaDelayInterval(targetKey, timestamp+delayTime);
            // consumerCommitMaxOffset();
            return true;
        } catch (Exception e) {
            consumer.pause(Collections.singletonList(topicPartition));
            consumer.seek(topicPartition, consumerRecord.offset());
            return false;
        }
    }

    private void delayMessageForwardFailRetryHandler(DelayMessage delayMessage) {
        String targetTopic = delayMessage.getTopic();
        String targetKey = delayMessage.getKey();
        String targetValue = delayMessage.getValue();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(targetTopic, targetKey, targetValue);
        try {
            this.producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (Objects.nonNull(recordMetadata)) {
                        log.info("retry send delay message to targetUser, topic:{}, key:{}, value:{}, offset:{}",
                                targetTopic, targetKey, targetValue, recordMetadata.offset());
                    } else {
                        // todo send delay message error logic
                        log.error("send delay message to targetUser error, topic:{}, key:{}, value:{}, reason:{}",
                                targetTopic, targetKey, targetValue,Objects.nonNull(e) ? e.getMessage() : "");
                        try {
                            delayMessageForwardFailQueue.put(delayMessage);
                        } catch (InterruptedException ex) {
                            log.error("delay message put retry queue error, {}", e.getMessage());
                        }
                    }

                }
            });
        } catch (Exception e) {
            log.error("send delay message to targetUser error, topic:{}, key:{}, value:{}, reason:{}",
                    targetTopic, targetKey, targetValue,Objects.nonNull(e) ? e.getMessage() : "");
            try {
                delayMessageForwardFailQueue.put(delayMessage);
            } catch (InterruptedException ex) {
                log.error("delay message put retry queue error, {}", e.getMessage());
            }
        }
    }

    private void consumerCommitMaxOffset() {
        if (Objects.isNull(this.topicPartition)) {
            return;
        }
        long waitCommitOffset = successMaxOffset.get();
        if(this.prevCommitOffset != waitCommitOffset) {
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(waitCommitOffset);
            Map<TopicPartition, OffsetAndMetadata> metadataMap = new HashMap<>();
            metadataMap.put(this.topicPartition, offsetAndMetadata);
            consumer.commitAsync(metadataMap, new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if(Objects.nonNull(map)) {
                        log.info("delay topic commit, topic:{}, partition:{},offset:{}",topicPartition.topic(), topicPartition.partition(), waitCommitOffset);
                        prevCommitOffset = waitCommitOffset;
                    }
                }
            });
        }
    }

    private void monitorKafkaDelayInterval(String key, long delayTime) {
        long now = System.currentTimeMillis();
        log.info("monitor delay interval, now:{}, delayTime:{}", now, delayTime);
        long monitorInterval = now - delayTime;
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.monitorTopic, key, String.valueOf(monitorInterval));
        this.producer.send(producerRecord);
    }
}
