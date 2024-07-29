package com.lin.manager.exception;

import com.lin.manager.common.CommonResponse;
import com.lin.manager.constants.MsgEnum;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * @author linzj
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(value = BizException.class)
    public ResponseEntity<CommonResponse> bizExceptionHandler(BizException exception) {
        return ResponseEntity.ok(CommonResponse.fail(exception.getMsgEnum()));
    }

    @ExceptionHandler(value = HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<CommonResponse> httpRequestMethodNotSupportedExceptionHandler(HttpRequestMethodNotSupportedException exception) {
        return ResponseEntity.ok(CommonResponse.fail(MsgEnum.BIZ_ERROR.getCode(), "接口请求方法不对,请校验"));
    }
}
