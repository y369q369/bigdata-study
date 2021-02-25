package com.example.springbootApi.config;

import com.example.springbootApi.vo.ResultVO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;

/**
 * @Author grassPrince
 * @Date 2020/11/9
 * @Description 全局异常处理
 **/
@RestControllerAdvice
@Slf4j
public class GlobalException {

    /**
     * 处理空指针的异常
     */
    @ExceptionHandler(value =NullPointerException.class)
    public ResultVO exceptionHandler(HttpServletRequest req, NullPointerException e){
        log.error("空指针异常，原因是：",e);
        return ResultVO.fail(e.getMessage());
    }

    /**
     * 处理所有异常
     */
    @ExceptionHandler
    public ResultVO exceptionHandler (HttpServletRequest request, Exception e) {
        log.error("未知异常，原因是：", e);
        return ResultVO.fail(e.getMessage());
    }

}
