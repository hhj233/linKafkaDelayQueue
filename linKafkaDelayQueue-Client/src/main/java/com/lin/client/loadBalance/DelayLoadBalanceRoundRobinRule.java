package com.lin.client.loadBalance;

import com.lin.common.constant.DelayLevelEnum;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author linzj
 */
public class DelayLoadBalanceRoundRobinRule implements DelayLoadBalanceRule {
    private final AtomicInteger position = new AtomicInteger(0);

    private static volatile DelayLoadBalanceRoundRobinRule instance;

    private DelayLoadBalanceRoundRobinRule() {
    }
    public static class DelayLoadBalanceRoundRobinRuleHandler {
        private static DelayLoadBalanceRoundRobinRule instance = new DelayLoadBalanceRoundRobinRule();
        public static DelayLoadBalanceRoundRobinRule getInstance() {
            return instance;
        }

    }



    @Override
    public String choose(DelayLevelEnum levelEnum, CopyOnWriteArrayList<String> delayTopicList) {
        List<String> delayLevelTopicList = delayTopicList.stream()
                .filter(i -> i.contains(levelEnum.getDesc())).sorted().collect(Collectors.toList());
        int pos = Math.abs(this.position.getAndIncrement());
        return delayLevelTopicList.get(pos % delayLevelTopicList.size());
    }
}
