package com.lin.manager.controller;

import com.lin.common.message.DelayMessage;
import com.lin.manager.common.CommonResponse;
import com.lin.manager.service.DelayRetryForwardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author linzj
 */
@RestController
@RequestMapping("delayRetry")
public class DelayRetryForwardController {
    @Autowired
    private DelayRetryForwardService delayRetryForwardService;
    @PostMapping("/forwardMessage")
    public CommonResponse delayRetryForward(@RequestBody DelayMessage delayMessage) {
        delayRetryForwardService.delayRetryForwardMessage(delayMessage);
        return CommonResponse.success();
    }
}
