package com.lin.core.helper;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.lin.common.constant.DelayConstEnum;
import com.lin.common.constant.DelayLevelEnum;
import com.lin.common.util.HashUtil;
import com.lin.common.util.JsonUtil;
import com.lin.common.util.NetUtil;
import com.lin.core.config.KafkaConfig;
import com.lin.core.registerCenter.DelayNacosDiscoveryInstance;
import com.lin.core.runner.DelayMessageRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author linzj
 */
@Slf4j
public class DelayMessageHelper {
    private final KafkaConfig kafkaConfig;
    private final DelayNacosDiscoveryInstance nacosDiscoveryInstance;
    private final ThreadPoolTaskExecutor delayThreadPoolExecutor;
    private final List<DelayMessageRunner> delayMessageRunnerList = new ArrayList<>();
    private final Map<String, DelayMessageRunner> delayMessageRunnerMap = new HashMap<>();
    private final SortedMap<Integer, String> delayCoreInstanceConsistentHashMap = new TreeMap<>();
    private final Set<String> prevDelayCoreInstance = new HashSet<>();
    private String currentInstanceId;
    private final Integer virtualNode = 1000;
    private final AdminClient adminClient;

    public DelayMessageHelper(KafkaConfig kafkaConfig, ThreadPoolTaskExecutor delayThreadPoolExecutor,
                              DelayNacosDiscoveryInstance nacosDiscoveryInstance) {
        Assert.notNull(kafkaConfig, "kafkaConfig cannot null");
        Assert.notNull(delayThreadPoolExecutor, "delayThreadPool cannot null");
        this.kafkaConfig = kafkaConfig;
        this.delayThreadPoolExecutor = delayThreadPoolExecutor;
        this.nacosDiscoveryInstance = nacosDiscoveryInstance;
        this.adminClient = createAdminClient();

    }

    public void start() {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                createDelayMessageRunner();
            }
        }, 0, 2000);
    }

    private void createDelayMessageRunner() {
        List<Instance> allDelayCoreInstance = this.nacosDiscoveryInstance.getAllInstance();
        if(CollectionUtils.isEmpty(allDelayCoreInstance)) {
            return;
        }
        // 服务实例增加,需分流
        if (!this.prevDelayCoreInstance.isEmpty()) {
            if (this.prevDelayCoreInstance.size() < allDelayCoreInstance.size()) {
                this.prevDelayCoreInstance.clear();
                log.info("shut down all delay transfer thread");
                shutdown();
            } else if (this.prevDelayCoreInstance.size() > allDelayCoreInstance.size()) {
                this.prevDelayCoreInstance.clear();
            } else {
                for (Instance instance : allDelayCoreInstance) {
                    if(!this.prevDelayCoreInstance.contains(instance.getInstanceId())) {
                        this.prevDelayCoreInstance.clear();
                        log.info("shut down all delay transfer thread");
                        shutdown();
                        break;
                    }
                }
            }
        }
        String localIp = NetUtil.getLocalIp();
        // 暂时加入接口进行识别
        String port = System.getenv("server.port");
        this.delayCoreInstanceConsistentHashMap.clear();
        for (Instance instance : allDelayCoreInstance) {
            String instanceId = instance.getInstanceId();
            this.prevDelayCoreInstance.add(instanceId);
            if(instance.getIp().equals(localIp) && instance.getPort() == Integer.parseInt(port)) {
                currentInstanceId = instanceId;
            }
            for (int i = 0; i < virtualNode; i++) {
                StringBuilder virtualNodeId = new StringBuilder(instanceId);
                virtualNodeId.append("&&").append(i);
                Integer consistentHashNode = HashUtil.FNV1_32_HASH(virtualNodeId.toString());
                this.delayCoreInstanceConsistentHashMap.put(consistentHashNode, virtualNodeId.toString());
            }
        }
        try {
            Set<String> allServerTopicSet = this.adminClient.listTopics().names().get();
            List<String> delayTopicList = allServerTopicSet.stream().filter(i -> i.contains(DelayConstEnum.CONST_DELAY_TOPIC_PREFIX.getCode()))
                    .collect(Collectors.toList());

            for (String delayTopic : delayTopicList) {
                String serverNode = getServerNode(delayTopic);
                if(serverNode.equals(currentInstanceId)) {
                    if (this.delayMessageRunnerMap.containsKey(delayTopic)) {
                        continue;
                    }
                    DelayLevelEnum delayLevelByTopic = DelayLevelEnum.getDelayLevelByTopic(delayTopic);
                    if (Objects.isNull(delayLevelByTopic)) {
                        continue;
                    }
                    String groupId = new StringBuilder(delayTopic).append("-group").toString();
                    DelayMessageRunner delayMessageRunner = new DelayMessageRunner(kafkaConfig.getBootstrapServers(),
                            groupId,kafkaConfig.getMonitorTopic() , delayTopic, delayLevelByTopic.getValue());
                    delayThreadPoolExecutor.execute(delayMessageRunner);
                    delayMessageRunnerList.add(delayMessageRunner);
                    delayMessageRunnerMap.put(delayTopic, delayMessageRunner);
                }
            }
            log.info("currentInstance:{}, run delay thread:{}", currentInstanceId, JsonUtil.toJsonString(this.delayMessageRunnerMap.keySet()));
        }catch (Exception e) {

        }

    }

    private AdminClient createAdminClient() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        AdminClient adminClient = KafkaAdminClient.create(prop);
        return adminClient;
    }

    private String getServerNode(String key) {
        int hashCode = HashUtil.FNV1_32_HASH(key);
        int hashServerNode = 0;
        SortedMap<Integer, String> tailNodeMap = this.delayCoreInstanceConsistentHashMap.tailMap(hashCode);
        if(tailNodeMap.isEmpty()) {
            hashServerNode = this.delayCoreInstanceConsistentHashMap.firstKey();
        }else {
            hashServerNode = tailNodeMap.firstKey();
        }
        String virtualServerNode = this.delayCoreInstanceConsistentHashMap.get(hashServerNode);
        return virtualServerNode.split("&&")[0];

    }

    public void shutdown() {
        for (DelayMessageRunner delayMessageRunner : delayMessageRunnerList) {
            delayMessageRunner.shutdown();
        }
        this.delayMessageRunnerList.clear();
        this.delayMessageRunnerMap.clear();
    }


}
