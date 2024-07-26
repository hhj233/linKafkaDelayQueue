package com.lin.core.delay;

import com.lin.core.config.KafkaConfig;
import com.lin.core.helper.DelayMessageHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * @author linzj
 */
@Component
public class DelayService {
    @Autowired
    private KafkaConfig kafkaConfig;
    @Autowired
    @Qualifier(value = "delayThreadPoolExecutor")
    private ThreadPoolTaskExecutor delayThreadPoolExecutor;
    private DelayMessageHelper delayMessageHelper;
    @PostConstruct
    public void init() {
        delayMessageHelper = new DelayMessageHelper(kafkaConfig, delayThreadPoolExecutor);
        delayMessageHelper.start();
    }

    @PreDestroy
    public void stop() {
        delayMessageHelper.shutdown();
    }
}
