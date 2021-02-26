package com.example.springbootApi.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

/**
 * @Author grassPrince
 * @Date 2021/2/25 17:27
 * @Description 列族元素获取数据对象
 **/
@Data
@ApiModel
public class HBaseGetCellModel {

    /** rowKey */
    @ApiModelProperty(value = "rowKey", example = "e9e868406875100a4d72322475d6d8eb1")
    private String row;

    /** 列族名称 */
    @ApiModelProperty(value = "列族名称", example = "info")
    private String family;

    /** 列族元素的名称 */
    @ApiModelProperty(value = "列族元素名称", example = "base")
    private String qualifier;

}
