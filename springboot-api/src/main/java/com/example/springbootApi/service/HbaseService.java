package com.example.springbootApi.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * @Author grassPrince
 * @Date 2021/2/7 16:12
 * @Description HBASE的操作业务类
 *  *              参考文档：
 **/
@Service
@Slf4j
public class HbaseService {

    /**
     * 创建Hbase连接
     * @return
     * @throws IOException
     */
    public Connection getHBaseAdmin(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "120.26.184.85");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error("创建HBase连接失败", e);
        }
        return connection;
    }

    public void getTableList() throws IOException {
        Connection connection = getHBaseAdmin();
        List<TableDescriptor> list = connection.getAdmin().listTableDescriptors();
        for (TableDescriptor tableDescriptor: list) {
            log.info("table: {}, columns: {}", tableDescriptor.getTableName(), tableDescriptor.getColumnFamilies().toString());
        }
    }


    public static void main(String[] args) throws IOException {
        HbaseService hbaseService = new HbaseService();
        hbaseService.getTableList();
    }
}
