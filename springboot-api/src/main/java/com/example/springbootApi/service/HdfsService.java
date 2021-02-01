package com.example.springbootApi.service;

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
        // 处理windows上hadoop_home找不到的报错
        System.setProperty("HADOOP_HOME", "F:\\Java\\hadoop-3.3.0");
        // 设置操作hadoop的用户，解决 org.apache.hadoop.security.AccessControlException: Permission denied: user=k
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.72.133:9000");
        FileSystem fs = FileSystem.get(configuration);
        return fs;
    }

    /**
     * 获取指定路径所有文件
     * @param dirPath 文件路径，例 hdfs://192.168.72.133:9000/test
     * @throws IOException
     */
    public FileStatus[] listStatus(String dirPath) throws IOException {
        FileSystem fs = getFileSystem();
        Path path = new Path(dirPath);
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            log.info("name:" + fileStatus.getPath().getName() + ", time: " + fileStatus.getModificationTime() + ", size:" + fileStatus.getLen());
        }
        return fileStatuses;
    }

    /**
     * 创建目录（默认递归创建）
     * @param dirPath 创建的文件路径
     * @throws IOException
     */
    public boolean mkdir(String dirPath) throws IOException {
        FileSystem fs = getFileSystem();
        Path path = new Path(dirPath);
        boolean mkdirFlag = fs.mkdirs(path);
        log.info("【hdfs操作】 目录： {} 创建 {}", dirPath, mkdirFlag ? "成功" : "失败");
        return mkdirFlag;
    }

    /**
     * 创建文件（目录不存在时，自动创建）
     * @param filePath 创建的文件路径
     * @throws IOException
     */
    public boolean createFile(String filePath) throws IOException {
        FileSystem fs = getFileSystem();
        Path path = new Path(filePath);
        boolean createFlag = fs.createNewFile(path);
        log.info("【hdfs操作】 文件： {} 创建 {}", filePath, createFlag ? "成功" : "失败");
        return createFlag;
    }

    /**
     * 删除文件或目录（默认递归删除）（不支持通配符，如 *）
     * @param deletePath 删除路径， 例：hdfs://192.168.72.133:9000/test3/test3_test
     * @return
     * @throws IOException
     */
    public boolean delete(String deletePath) throws IOException {
        FileSystem fs = getFileSystem();
        Path path = new Path(deletePath);
        // 第二个参数为true时强制删除， false判断目录是否为空
        boolean deleteFlag = fs.delete(path, true);
        log.info("【hdfs操作】 文件： {} 删除 {}", deletePath, deleteFlag ? "成功" : "失败");
        return deleteFlag;
    }

    /**
     * 上传文件
     * @param uploadPath 待上传文件路径（只能为文件，不可为目录）
     * @param targetPath 上传目标路径（1.目录不存在自动创建，2.路径的最后一个/后的名称即为上传后的文件名）
     * @return
     * @throws IOException
     */
    public void upload(String uploadPath, String targetPath) throws IOException {
        FileSystem fs = getFileSystem();
        fs.copyFromLocalFile(new Path(uploadPath), new Path(targetPath));
    }

    /**
     * 下载文件或目录
     * @param downloadPath 待下载文件路径（既可为目录也可为文件名）
     * @param targetPath 下载目标路径 (目录存在时，文件名为待下载文件名，2.目录不存在时，递归创建目录，最后一个目录或文件名即为下载后的文件名， 3. 下载的文件会附带.crc结尾的文件，用于文件完整性校验)
     * @throws IOException
     */
    public void download(String downloadPath, String targetPath) throws IOException {
        FileSystem fs = getFileSystem();
        fs.copyToLocalFile(new Path(downloadPath), new Path(targetPath));
    }


    public static void main(String[] args) throws IOException {
        HdfsService hdfsService = new HdfsService();
//        hdfsService.listStatus("hdfs://192.168.72.133:9000/test");
//        hdfsService.mkdir("hdfs://192.168.72.133:9000/test3/test3_test");
        hdfsService.createFile("hdfs://192.168.72.133:9000/test2/test3/test.txt");
//        hdfsService.delete("hdfs://192.168.72.133:9000/test4");
//        hdfsService.upload("C:\\Users\\grassprince\\Desktop\\test.txt", "hdfs://192.168.72.133:9000/test4/");
//        hdfsService.download("hdfs://192.168.72.133:9000/test/test1.txt", "G:\\" );
    }

}
