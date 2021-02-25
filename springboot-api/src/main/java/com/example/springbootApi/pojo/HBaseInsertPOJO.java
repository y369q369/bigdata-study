package com.example.springbootApi.pojo;

import lombok.Data;
import org.apache.hadoop.hbase.TableName;

import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/25 17:23
 * @Description HBase插入数据对象
 **/
@Data
public class HBaseInsertPOJO {

    /** rowKey */
    private TableName tableName;

    /** rowKey对应的值 */
    private List<HBaseRowKeyPOJO> rows;

}
