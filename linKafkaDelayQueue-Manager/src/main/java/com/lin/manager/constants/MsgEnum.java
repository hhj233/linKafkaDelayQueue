package com.lin.manager.constants;

import lombok.Getter;

/**
 * @author linzj
 */
@Getter
public enum MsgEnum {
    SUCCESS(200, "success"),
    BIZ_ERROR(4001000, "system error")
    ;
    private Integer code;
    private String desc;

    MsgEnum(Integer code, String desc) {
        this.code = code;
        this.desc = desc;
    }
}
