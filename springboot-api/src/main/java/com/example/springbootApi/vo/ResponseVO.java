package com.example.springbootApi.vo;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * @Author grassPrince
 * @Date 2020/7/16 14:16
 * @Description 返回给前台的统一类
 **/
@Data
@ApiModel(value = "通用接口返回对象")
public class ResponseVO {

    /** 返回状态 */
    @ApiModelProperty(required = true, notes = "返回状态", example = "true")
    private boolean status;

    /** 返回数据 */
    @ApiModelProperty(required = true, notes = "返回信息")
    private Object message;

    /**
     * 成功
     */
    public static ResponseVO success(Object message) {
        ResponseVO responseVO = new ResponseVO();
        responseVO.setStatus(true);
        responseVO.setMessage(message);
        return responseVO;
    }

    /**
     * 失败
     */
    public static ResponseVO fail(Object message) {
        ResponseVO responseVO = new ResponseVO();
        responseVO.setStatus(false);
        responseVO.setMessage(message);
        return responseVO;
    }

}
