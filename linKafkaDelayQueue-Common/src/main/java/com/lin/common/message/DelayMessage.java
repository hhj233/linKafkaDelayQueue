package com.lin.common.message;

import com.lin.common.constant.DelayLevelEnum;
import lombok.Data;

/**
 * @author linzj
 */
@Data
public class DelayMessage {
    /**
     * 目标主题
     */
    private String topic;

    /**
     * 目标消息key
     */
    private String key;

    /**
     * 目标消息value
     */
    private String value;

    /**
     * 延时水平
     */
    private DelayLevelEnum level;

}
