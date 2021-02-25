package com.example.springbootApi.vo;

import lombok.Data;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/25 16:12
 * @Description hbase的表信息 视图
 **/
@Data
public class HBaseTableVO {

    /** 表名 */
    private String name;

    /** 列名 */
    private List<String> families;

    public HBaseTableVO(String name, ColumnFamilyDescriptor[] ColumnFamilyDescriptors) {
        this.name = name;
        this.families = new ArrayList<>();
        for (ColumnFamilyDescriptor columnFamilyDescriptor : ColumnFamilyDescriptors) {
            this.families.add(columnFamilyDescriptor.getNameAsString());

        }
    }
}
