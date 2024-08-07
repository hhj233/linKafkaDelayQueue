package com.lin.client.loadBalance;

import com.lin.common.constant.DelayLevelEnum;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author linzj
 */
public interface DelayLoadBalanceRule {

    /**
     * 负载均衡规则路由
     * @param level
     * @Param delayTopicList
     * @return
     */
    String choose(DelayLevelEnum level, CopyOnWriteArrayList<String> delayTopicList);
}
