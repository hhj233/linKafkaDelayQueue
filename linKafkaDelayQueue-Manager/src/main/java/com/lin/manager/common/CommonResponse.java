package com.lin.manager.common;

import com.lin.manager.constants.MsgEnum;
import lombok.Data;


/**
 * @author linzj
 */
@Data
public class CommonResponse<T> {
    private static final int SUCCESS = 200;
    private int code;
    private String msg;
    private String desc;
    private T data;

    public static <T> CommonResponse success(T data) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setCode(200);
        commonResponse.setMsg("success");
        commonResponse.setDesc("success");
        commonResponse.setData(data);
        return commonResponse;
    }

    public static CommonResponse success() {
        return success(null);
    }

    public static CommonResponse fail(int code, String msg) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setCode(code);
        commonResponse.setMsg(msg);
        commonResponse.setDesc(msg);
        return commonResponse;
    }

    public static CommonResponse fail(MsgEnum msgEnum) {
        CommonResponse commonResponse = new CommonResponse();
        commonResponse.setCode(msgEnum.getCode());
        commonResponse.setMsg(msgEnum.getDesc());
        commonResponse.setDesc(msgEnum.getDesc());
        return commonResponse;
    }

}
