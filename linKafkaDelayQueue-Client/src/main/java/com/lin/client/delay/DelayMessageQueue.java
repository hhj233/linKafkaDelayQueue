package com.lin.client.delay;

import com.lin.client.loadBalance.DelayLoadBalanceRoundRobinRule;
import com.lin.common.config.DelayMessageConfig;
import com.lin.common.constant.DelayConstEnum;
import com.lin.common.constant.DelayLevelEnum;
import com.lin.common.message.DelayMessage;
import com.lin.common.util.ExceptionUtil;
import com.lin.common.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * @author linzj
 */
@Slf4j
public class DelayMessageQueue {
    private final DelayMessageConfig config;

    private final AdminClient adminClient;
    private CopyOnWriteArrayList<String> delayTopicList = new CopyOnWriteArrayList<>();

    private KafkaConsumer<String,String> consumer;
    private KafkaProducer<String,String> producer;

    public DelayMessageQueue(DelayMessageConfig config, AdminClient client) {
        this.config = config;
        this.adminClient = client;

    }

    public void init() {
        this.consumer = createKafkaConsumer(config.getServers(), config.getGroupId());
        this.producer = createKafkaProducer(config.getServers());
        this.consumer.subscribe(Collections.singleton(config.getTopic()));
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                delayTopicQuery();
            }
        }, 0, 60*1000);
    }

    private void delayTopicQuery() {
        try {
            Set<String> allServerTopicSet = this.adminClient.listTopics().names().get();
            List<String> delayTopicList = allServerTopicSet.stream().filter(i -> i.contains(DelayConstEnum.CONST_DELAY_TOPIC_PREFIX.getCode()))
                    .collect(Collectors.toList());
            for (String delayTopic : delayTopicList) {
                boolean flag = false;
                for (String alreadyDelayTopic : this.delayTopicList) {
                    if (alreadyDelayTopic.equals(delayTopic)) {
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    this.delayTopicList.add(delayTopic);

                }
            }
        } catch (InterruptedException e) {
            log.error("get delay topic error:{}", ExceptionUtil.getStackTraceAsString(e));
        } catch (ExecutionException e) {
            log.error("get delay topic error:{}", ExceptionUtil.getStackTraceAsString(e));
        }
    }

    public void shutdown() {
        this.consumer.close();
        this.producer.close();
    }
    public void offer(String message, DelayLevelEnum level) {
        DelayMessage delayMessage = new DelayMessage();
        delayMessage.setLevel(level);
        delayMessage.setTopic(config.getTopic());
        delayMessage.setValue(message);
        // todo 负载均衡器
        String delayTopic = DelayLoadBalanceRoundRobinRule.DelayLoadBalanceRoundRobinRuleHandler
                .getInstance().choose(level, this.delayTopicList);
        log.info("load balance delay level topic,levelTopic:{}, chooseTopic:{}", level.getDesc(), delayTopic);
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(delayTopic,
                delayMessage.getKey(), JsonUtil.toJsonString(delayMessage));
        this.producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(Objects.nonNull(recordMetadata)) {
                    log.info("send delay message to delayTopic, topic:{}, key:{}, value:{}, offset:{}",
                            level.getDesc(), delayMessage.getKey(), JsonUtil.toJsonString(delayMessage), recordMetadata.offset());
                } else {
                    log.error("send Kafka error:{}", e.getMessage());
                }
            }
        });
    }

    public void take(Consumer<String> consumer) {
        try {
            while(true) {
                ConsumerRecords<String, String> consumerRecords = this.consumer.poll(Duration.ofMillis(200));
                if (consumerRecords.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    String value = consumerRecord.value();
                    try {
                        consumer.accept(value);
                    }catch (Exception e) {
                        log.warn("biz consumer occur exception, {}", ExceptionUtil.getStackTraceAsString(e));
                    }
                }
                this.consumer.commitAsync();
            }
        } catch (Exception e) {
            log.warn("delay message queue take occur exception, :{}", ExceptionUtil.getStackTraceAsString(e));
        }finally {
            this.consumer.commitSync();
        }
    }

    private KafkaConsumer<String,String> createKafkaConsumer(String servers, String groupId) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
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
