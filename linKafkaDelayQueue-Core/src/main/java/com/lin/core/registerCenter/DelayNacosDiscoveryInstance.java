package com.lin.core.registerCenter;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.lin.common.util.ExceptionUtil;
import com.lin.common.util.JsonUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;


/**
 * @author linzj
 */
@Component
@Slf4j
@Data
public class DelayNacosDiscoveryInstance {

    @Value("${spring.application.name}")
    private String delayBootCoreName;

    @Value("${spring.cloud.nacos.discovery.server-addr}")
    private String nacosAddr;

    private NamingService namingService;

    @PostConstruct
    public void init() {
        namingService = createNamingService();
    }


    public List<Instance> getAllInstance() {
        try {
            return namingService.getAllInstances(this.delayBootCoreName);
        } catch (Exception e) {
            log.warn("get delay core boot instance error:{}", ExceptionUtil.getStackTraceAsString(e));
        }
        return null;
    }

    private NamingService createNamingService() {
        try {
            NamingService namingService = NamingFactory.createNamingService(nacosAddr);
            return namingService;
        } catch (NacosException e) {
            log.error("create a nacos instance error:{}", ExceptionUtil.getStackTraceAsString(e));
        }
        return null;
    }

    public static void main(String[] args) {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            System.out.println(localHost.getHostAddress());
            NamingService namingService = NamingFactory.createNamingService("127.0.0.1:8848");
            List<Instance> allInstances = namingService.getAllInstances("linKafkaDelayQueue-Core");
            System.out.println(JsonUtil.toJsonString(allInstances));
        } catch (NacosException e) {
            log.error("create a nacos instance error:{}", ExceptionUtil.getStackTraceAsString(e));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
