package com.example.bigdatastudy.service;

import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;


/**
 * @Author grassPrince
 * @Date 2021/1/6 16:38
 * @Description hdfs的操作业务类
 *              参考文档： http://c.biancheng.net/view/3576.html
 **/
@Service
@Slf4j
public class HdfsService {

    public FileSystem getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.72.133:9000");
        FileSystem fs = FileSystem.get(configuration);
        return fs;
    }

    public Path getHomeDirectory() throws IOException {
        FileSystem fs = getFileSystem();
        Path path = fs.getHomeDirectory();
        return path;
    }

    public void listStatus() throws IOException {
        FileSystem fs = getFileSystem();
        FileStatus[] fileStatuses = fs.listStatus(fs.getHomeDirectory());
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("name:" + fileStatus.getPath().getName() + "/t/tsize:" + fileStatus.getLen());
        }
    }

    public boolean mkdir(String dirPath) throws IOException {
        FileSystem fs = getFileSystem();
        Path path = new Path(dirPath);
        boolean mkdirFlag = fs.mkdirs(path);
        log.info("目录： {} 创建 {}", dirPath, mkdirFlag ? "成功" : "失败");
        return mkdirFlag;
    }



}
