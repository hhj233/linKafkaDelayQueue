package com.lin.manager.service.impl;

import com.lin.common.message.DelayMessage;
import com.lin.manager.service.DelayRetryForwardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * @author linzj
 */
@Service
public class DelayRetryForwardServiceImpl implements DelayRetryForwardService {
    @Autowired
    private KafkaTemplate kafkaTemplate;
    @Override
    public void delayRetryForwardMessage(DelayMessage req) {
        kafkaTemplate.send(req.getTopic(), req.getValue());
    }
}
