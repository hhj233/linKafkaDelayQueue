package com.lin.manager.exception;

import com.lin.manager.constants.MsgEnum;
import lombok.Data;

/**
 * @author linzj
 */
@Data
public class BizException extends RuntimeException {
    private MsgEnum msgEnum;

    public BizException(MsgEnum msgEnum) {
        super(msgEnum.getDesc());
        this.msgEnum = msgEnum;
    }
}
