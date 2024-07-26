package com.lin.common.constant;

import lombok.Getter;

/**
 * @author linzj
 */
@Getter
public enum DelayLevelEnum {
    DELAY_SECOND_1(1000L, "kafka__delay-second-1"),
    DELAY_SECOND_5(1000L*5, "kafka__delay-second-5"),
    DELAY_SECOND_10(1000L*10, "kafka__delay-second-10"),
    DELAY_SECOND_30(1000L*30, "kafka__delay-second-30"),
    DELAY_MINUTE_1(1000L * 60, "kafka__delay-minute-1"),
    DELAY_MINUTE_2(1000L*60*2, "kafka__delay-minute-2"),
    DELAY_MINUTE_3(1000L*60*3, "kafka__delay-minute-3"),
    DELAY_MINUTE_4(1000L*60*4, "kafka__delay-minute-4"),
    DELAY_MINUTE_5(1000L*60*5, "kafka__delay-minute-5"),
    DELAY_MINUTE_6(1000L*60*6, "kafka__delay-minute-6"),
    DELAY_MINUTE_7(1000L*60*7, "kafka__delay-minute-7"),
    DELAY_MINUTE_8(1000L*60*8, "kafka__delay-minute-8"),
    DELAY_MINUTE_9(1000L*60*9, "kafka__delay-minute-9"),
    DELAY_MINUTE_10(1000L*60*10, "kafka__delay-minute-10"),
    DELAY_MINUTE_15(1000L*60*15, "kafka__delay-minute-15"),
    DELAY_MINUTE_20(1000L*60*20, "kafka__delay-minute-20"),
    DELAY_MINUTE_30(1000L*60*30, "kafka__delay-minute-30"),
    ;

    /**
     * 延时时长/ms
     */
    private Long value;

    /**
     * 延时desc
     */
    private String desc;

    DelayLevelEnum(Long value, String desc) {
        this.value = value;
        this.desc = desc;
    }
}
