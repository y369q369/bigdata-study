package com.example.springbootApi.model;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/25 15:05
 * @Description HBase的测试表
 **/
@Data
@ApiModel
public class HBaseTableModel {

    @ApiModelProperty(value = "表名", example = "test")
    private String name;

    @ApiModelProperty(value = "列族名称集合")
    private List<String> families;

//    private String qualifier;


    public HBaseTableModel(String name, List<String> families) {
        this.name = name;
        this.families = families;
    }
}
