package com.example.springbootApi.model;

import com.example.springbootApi.pojo.HBaseColumnPOJO;
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
public class HBaseInsertModel {

    @ApiModelProperty(value = "表名", example = "test")
    private String tableName;

    /** rowKey对应的值 */
    private List<HBaseRowKeyModel> rows;

    /**
     * 将 HBaseInsertModel 转换为 HBaseInsertPOJO
     * @return
     */
    public HBaseInsertPOJO convert() {
        HBaseInsertPOJO insert = new HBaseInsertPOJO();
        insert.setTableName(TableName.valueOf(tableName));
        List<HBaseRowKeyPOJO> rowKeys = new ArrayList<>();
        insert.setRows(rowKeys);
        for (HBaseRowKeyModel rowKeyModel : rows) {
            HBaseRowKeyPOJO rowKeyPOJO = new HBaseRowKeyPOJO();
            rowKeyPOJO.setRow(rowKeyModel.getRow().getBytes());
            List<HBaseColumnPOJO> columns = new ArrayList<>();
            for (HBaseColumnModel columnModel: rowKeyModel.getColumns()) {
                HBaseColumnPOJO columnPOJO = new HBaseColumnPOJO();
                columnPOJO.setFamily(columnModel.getFamily().getBytes());
                columnPOJO.setQualifier(columnModel.getQualifier().getBytes());
                columnPOJO.setValue(columnModel.getValue().getBytes());
                columns.add(columnPOJO);
            }
            rowKeyPOJO.setColumns(columns);
            rowKeys.add(rowKeyPOJO);
        }
        return insert;
    }

}
