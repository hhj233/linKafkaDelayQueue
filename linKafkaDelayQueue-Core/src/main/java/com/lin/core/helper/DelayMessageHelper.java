package com.lin.core.helper;

import com.lin.common.constant.DelayLevelEnum;
import com.lin.core.config.KafkaConfig;
import com.lin.core.runner.DelayMessageRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author linzj
 */
@Slf4j
public class DelayMessageHelper {
    private final KafkaConfig kafkaConfig;
    private final ThreadPoolTaskExecutor delayThreadPoolExecutor;
    private final List<DelayMessageRunner> delayMessageRunnerList = new ArrayList<>();

    public DelayMessageHelper(KafkaConfig kafkaConfig, ThreadPoolTaskExecutor delayThreadPoolExecutor) {
        Assert.notNull(kafkaConfig, "kafkaConfig cannot null");
        Assert.notNull(delayThreadPoolExecutor, "delayThreadPool cannot null");
        this.kafkaConfig = kafkaConfig;
        this.delayThreadPoolExecutor = delayThreadPoolExecutor;

    }

    public void start() {
        Arrays.stream(DelayLevelEnum.values()).forEach(i -> {
            DelayMessageRunner delayMessageRunner = new DelayMessageRunner(kafkaConfig.getBootstrapServers(),
                    kafkaConfig.getConsumer().get("group-id").toString(), i.getDesc(), i.getValue());
            delayThreadPoolExecutor.execute(delayMessageRunner);
            delayMessageRunnerList.add(delayMessageRunner);
        });
    }

    public void shutdown() {
        for (DelayMessageRunner delayMessageRunner : delayMessageRunnerList) {
            delayMessageRunner.shutdown();
        }
        this.delayMessageRunnerList.clear();
    }


}
