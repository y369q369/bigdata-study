package com.example.springbootApi.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.stereotype.Service;

/**
 * @Author grassPrince
 * @Date 2021/2/7 16:12
 * @Description HBASE的操作业务类
 *  *              参考文档：
 **/
@Service
public class HbaseService {

    public void getHBaseAdmin() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "");
    }

}
