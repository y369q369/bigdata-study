package com.example.springbootApi.model;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * @Author grassPrince
 * @Date 2021/2/25 17:27
 * @Description 列组元素数据对象
 **/
@Data
public class HBaseColumnModel {

    /** 列族名称 */
    @ApiModelProperty(value = "列族名称", example = "info")
    private String family;

    /** 列族元素的名称 */
    @ApiModelProperty(value = "列族元素名称", example = "base")
    private String qualifier;

    /** 列族元素的值 */
    @ApiModelProperty(value = "列族元素的值", example = "hh")
    private String value;

}
