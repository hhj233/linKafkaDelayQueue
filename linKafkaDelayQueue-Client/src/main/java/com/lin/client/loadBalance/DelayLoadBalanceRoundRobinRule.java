package com.lin.client.loadBalance;

import com.lin.common.constant.DelayLevelEnum;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author linzj
 */
public class DelayLoadBalanceRoundRobinRule implements DelayLoadBalanceRule {
    private final Map<String, AtomicInteger> levelPositionMap;

    private static volatile DelayLoadBalanceRoundRobinRule instance;

    private DelayLoadBalanceRoundRobinRule() {
        levelPositionMap = new HashMap<>();
        for (DelayLevelEnum level : DelayLevelEnum.values()) {
            levelPositionMap.put(level.getDesc(), new AtomicInteger(0));
        }
    }
    public static class DelayLoadBalanceRoundRobinRuleHandler {
        private static DelayLoadBalanceRoundRobinRule instance = new DelayLoadBalanceRoundRobinRule();
        public static DelayLoadBalanceRoundRobinRule getInstance() {
            return instance;
        }

    }



    @Override
    public String choose(DelayLevelEnum levelEnum, CopyOnWriteArrayList<String> delayTopicList) {
        AtomicInteger position = levelPositionMap.get(levelEnum.getDesc());
        List<String> delayLevelTopicList = delayTopicList.stream()
                .filter(i -> i.contains(levelEnum.getDesc())).sorted().collect(Collectors.toList());
        int pos = Math.abs(position.getAndIncrement());
        return delayLevelTopicList.get(pos % delayLevelTopicList.size());
    }
}
