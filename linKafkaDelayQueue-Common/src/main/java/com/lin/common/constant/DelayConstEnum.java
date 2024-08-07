package com.lin.common.constant;

import lombok.Getter;

/**
 * @author linzj
 */

@Getter
public enum DelayConstEnum {
    CONST_DELAY_TOPIC_PREFIX("kafka__delay")
    ;
    private String code;

    DelayConstEnum(String code) {
        this.code = code;
    }
}
