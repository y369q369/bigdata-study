package com.example.springbootApi.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.*;

/**
 * @Author grassPrince
 * @Date 2021/2/23 16:11
 * @Description io业务类
 **/
@Service
@Slf4j
public class IOService {

    /**
     * 根据文件路径读取文件
     * @param filePath 文件路径
     * @throws IOException
     */
    public void readFile(String filePath) throws IOException {
        File file=new File(filePath);
        InputStreamReader in_stream = new InputStreamReader(new FileInputStream(file));
        BufferedReader in = new BufferedReader(in_stream);
        String s;
        while (( s = in.readLine()) != null ) {
            log.info(s);
        }
    }

    public void readFile2(String filePath) throws IOException {
        File file = new File(filePath);
        int size = 200;
        // 接收数据的对象，多次读取时覆盖上一次的读取内容
        byte[] buff = new byte[size];
        int lg = -1;
        // 读入文件流
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
        // 一个文件块一个文件块地读
        while ((lg = in.read(buff)) != -1) {
            // 当读取长度达不到预定长度，多出的长度内容为上一次读取的内容，须去除
            if (lg != size) {
                byte[] bufftmp = new byte[lg];
                // 复制lg长度的内容到新临时数组
                System.arraycopy(buff, 0, bufftmp, 0, lg);
                log.info("bufftmp: {}", Bytes.toString(bufftmp));
            } else {
                log.info("buff: {}", Bytes.toString(buff));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        IOService ioService = new IOService();
//        ioService.readFile("C:\\Users\\grassprince\\Desktop\\src2\\main\\java\\io\\transwarp\\config\\Test.java");
        ioService.readFile2("C:\\Users\\grassprince\\Desktop\\src2\\main\\java\\io\\transwarp\\config\\Test.java");
    }
}
