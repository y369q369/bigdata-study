package com.example.springbootApi.pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @Author grassPrince
 * @Date 2021/2/25 17:23
 * @Description rowKey对应数据对象
 **/
@Data
public class HBaseRowKeyPOJO {

    /** rowKey */
    private byte[] row;

    /** rowKey对应的值 */
    private List<HBaseColumnPOJO> columns;

}
