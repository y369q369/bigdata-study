package com.example.springbootApi.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Random;

/**
 * @Author grassPrince
 * @Date 2021/2/23 16:11
 * @Description io业务类
 **/
@Service
@Slf4j
public class IOService {

    /**
     * 字符流 根据文件路径读取文件
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

    /**
     * 字节流 根据文件路径读取文件
     * @param filePath 文件路径
     * @throws IOException
     */
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

    /**
     * 字符流 写入
     * @param filePath 文件路径
     * @throws IOException
     */
    public void write(String filePath) throws IOException {
        // 直接覆盖文件内容
//        FileWriter fw = new FileWriter(filePath);
        // 文件中追加
        FileWriter fw = new FileWriter(filePath, true);
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 0; i < 10; i++) {
            bw.write(String.valueOf(new Random().nextInt(1000000)));
            bw.newLine();
        }

        if (bw != null) {
            bw.close();
        }
        if (fw != null) {
            fw.close();
        }
        log.info("字符流写入完成");
    }

    /**
     * 字节流 写入
     * @param filePath 文件路径
     * @throws IOException
     */
    public void write2(String filePath) throws IOException {
        // 覆盖原有内容
//        FileOutputStream fs = new FileOutputStream(filePath);
        // 追加内容
        FileOutputStream fs = new FileOutputStream(filePath, true);
        for (int i = 0; i < 10; i++) {
            fs.write(String.valueOf(new Random().nextInt(1000000)).getBytes());
            fs.write("\n".getBytes());
        }
        log.info("字节流写入完成");
    }

    public static void main(String[] args) throws IOException {
        IOService ioService = new IOService();
//        ioService.readFile("C:\\Users\\grassprince\\Desktop\\src2\\main\\java\\io\\transwarp\\config\\Test.java");
//        ioService.readFile2("C:\\Users\\grassprince\\Desktop\\src2\\main\\java\\io\\transwarp\\config\\Test.java");
//        ioService.write("./test.txt");
        ioService.write2("./test2.txt");
    }
}
