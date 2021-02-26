package com.example.springbootApi.po;

import com.example.springbootApi.pojo.HBaseCellPOJO;
import com.example.springbootApi.pojo.HBaseInsertPOJO;
import com.example.springbootApi.pojo.HBaseRowKeyPOJO;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.hadoop.hbase.TableName;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/25 17:23
 * @Description HBase插入数据对象
 **/
@Data
public class HBaseTablePO {

    @ApiModelProperty(value = "表名", example = "test")
    private String tableName;

    /** rowKey对应的值 */
    private List<HBaseRowKeyPO> rows;

    /**
     * 将 HBaseInsertModel 转换为 HBaseInsertPOJO
     * @return
     */
    public HBaseInsertPOJO convert() {
        HBaseInsertPOJO insert = new HBaseInsertPOJO();
        insert.setTableName(TableName.valueOf(tableName));
        List<HBaseRowKeyPOJO> rowKeys = new ArrayList<>();
        insert.setRows(rowKeys);
        for (HBaseRowKeyPO rowKeyModel : rows) {
            HBaseRowKeyPOJO rowKeyPOJO = new HBaseRowKeyPOJO();
            rowKeyPOJO.setRow(rowKeyModel.getRow().getBytes());
            List<HBaseCellPOJO> cells = new ArrayList<>();
            for (HBaseCellPO cellModel: rowKeyModel.getCells()) {
                cells.add(new HBaseCellPOJO(cellModel.getFamily().getBytes(), cellModel.getQualifier().getBytes(), cellModel.getValue().getBytes()));
            }
            rowKeyPOJO.setCells(cells);
            rowKeys.add(rowKeyPOJO);
        }
        return insert;
    }

}
