package com.example.springbootApi.pojo;

import lombok.Data;

import java.util.List;

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
    private List<HBaseCellPOJO> cells;

    public HBaseRowKeyPOJO(byte[] row, List<HBaseCellPOJO> cells) {
        this.row = row;
        this.cells = cells;
    }

    public HBaseRowKeyPOJO() {
    }
}
