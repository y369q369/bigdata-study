package com.example.springbootApi.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/26 8:57
 * @Description 扫描数据对象
 **/
@Data
@ApiModel
public class HBaseScanModel {

    @ApiModelProperty(value = "查询表名", example = "test", required = true)
    private String name;

    @ApiModelProperty(value = "扫描开始的row", example = "e9e868406875100a4d72322475d6d8eb1")
    private String startRow;

    @ApiModelProperty(value = "扫描结束的row", example = "e9e868406875100a4d72322475d6d8eb99")
    private String stopRow;

    @ApiModelProperty(value = "扫描的列族和列族元素集合")
    private List<HBaseScanCellModel> cells;

}
