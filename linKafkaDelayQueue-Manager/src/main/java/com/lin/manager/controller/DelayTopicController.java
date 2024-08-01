package com.lin.manager.controller;

import com.lin.manager.common.CommonResponse;
import com.lin.manager.dto.DelayTopicDetailDto;
import com.lin.manager.service.DelayTopicService;
import com.lin.manager.vo.DelayTopicDetailRespVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author linzj
 */
@RestController
@RequestMapping(value = "delayTopic")
public class DelayTopicController {

    @Autowired
    private DelayTopicService delayTopicService;


    @PostMapping("/list")
    public CommonResponse getAllTopic() {
        return CommonResponse.success(delayTopicService.allDelayTopic());
    }

    @PostMapping("/detail")
    public CommonResponse<DelayTopicDetailRespVo> detail(@RequestBody DelayTopicDetailDto req) {
        return CommonResponse.success(delayTopicService.delayTopicDetail(req));
    }

}
