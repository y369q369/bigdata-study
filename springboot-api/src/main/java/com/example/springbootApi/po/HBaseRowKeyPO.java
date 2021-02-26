package com.example.springbootApi.po;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/25 17:23
 * @Description rowKey对应数据对象
 **/
@Data
public class HBaseRowKeyPO {

    /** rowKey */
    @ApiModelProperty(value = "row", example = "123456")
    private String row;

    /** rowKey对应的值 */
    private List<HBaseCellPO> cells;

}
