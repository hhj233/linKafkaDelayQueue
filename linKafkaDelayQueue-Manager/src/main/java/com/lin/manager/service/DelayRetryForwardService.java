package com.lin.manager.service;

import com.lin.common.message.DelayMessage;

/**
 * @author linzj
 */
public interface DelayRetryForwardService {
    /**
     * retry forward message
     * @param req
     */
    void delayRetryForwardMessage(DelayMessage req);
}
