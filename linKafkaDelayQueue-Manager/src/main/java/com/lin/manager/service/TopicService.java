package com.lin.manager.service;

import com.lin.manager.dto.DelayTopicDetailDto;
import com.lin.manager.vo.DelayTopicDetailRespVo;

import java.util.List;

/**
 * @author linzj
 */
public interface TopicService {
    /**
     * 获取所有topic
     * @return
     */
    List<String> allDelayTopic();

    /**
     * 延迟主题详情
     * @param req
     * @return
     */
    DelayTopicDetailRespVo delayTopicDetail(DelayTopicDetailDto req);


}
