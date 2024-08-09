package com.lin.common.util;

import com.lin.common.constant.DelayLevelEnum;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.SortedMap;
import java.util.TreeMap;

@RunWith(value = MockitoJUnitRunner.class)
@Slf4j
public class HashUtilTest {
    private static String[] serverNodes = {"192.168.177.1#8081#DEFAULT#DEFAULT_GROUP@@linKafkaDelayQueue-Core"
            , "192.168.177.1#8082#DEFAULT#DEFAULT_GROUP@@linKafkaDelayQueue-Core"
            ,"192.168.177.1#8083#DEFAULT#DEFAULT_GROUP@@linKafkaDelayQueue-Core"};
    private static SortedMap<Integer, String> map = new TreeMap<>();

    private final static int virtualNode = 5;

    static {
        for (String serverNode : serverNodes) {
            for(int i = 0; i < virtualNode; i++) {
                StringBuilder sb = new StringBuilder(serverNode);
                sb.append("&&").append(i);
                int hash = HashUtil.FNV1_32_HASH(sb.toString());
                log.info("serverNode:{}, hash:{}", sb.toString(), hash);
                map.put(hash, serverNode);
            }
        }
    }

    
    public String getServerNode(String key) {
        int hash = HashUtil.FNV1_32_HASH(key);
        log.info("{}-{}", key, hash);
        SortedMap<Integer, String> sortMap = map.tailMap(hash);
        int hashNode = 0;
        if (sortMap.isEmpty()) {
            hashNode = map.firstKey();
        } else {
            hashNode = sortMap.firstKey();
        }
        String virtualNode = map.get(hashNode);
        return virtualNode.split("&&")[0];
    }

    @Test
    public void testConstantHash() {
        for (DelayLevelEnum value : DelayLevelEnum.values()) {
            String serverNode = getServerNode(value.getDesc());
            log.info("{}:node:{}", value.getDesc(), serverNode);
        }
        for(int i = 0; i < 3; i++) {
            String key = DelayLevelEnum.DELAY_MINUTE_1.getDesc() + "-" + i;
            String serverNode = getServerNode(key);
            log.info("{}:node:{}", key, serverNode);
        }
        String localIp = NetUtil.getLocalIp();
        System.out.println(localIp);

    }
}