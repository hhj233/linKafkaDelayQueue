package com.lin.manager.controller;

import com.lin.manager.common.CommonResponse;
import com.lin.manager.service.DelayMonitorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author linzj
 */
@RestController
@RequestMapping("/delayMessageMonitor")
public class DelayMonitorController {
    @Autowired
    private DelayMonitorService delayMonitorService;
    @PostMapping("/monitor")
    public CommonResponse delayMessageMonitor() {
        return CommonResponse.success( delayMonitorService.delayMessageIntervalMonitor());
    }
}
