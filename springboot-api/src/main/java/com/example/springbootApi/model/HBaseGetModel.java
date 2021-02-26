package com.example.springbootApi.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/26 8:57
 * @Description 获取数据对象
 **/
@Data
@ApiModel
public class HBaseGetModel {

    @ApiModelProperty(value = "查询表名", example = "test", required = true)
    private String name;

    @ApiModelProperty(value = "row,列族和列族元素集合")
    private List<HBaseGetCellModel> cells;

}
