package com.example.springbootApi.utils;

import com.example.springbootApi.constant.UploadTable;
import com.example.springbootApi.model.HBaseTableModel;
import com.example.springbootApi.pojo.HBaseCellPOJO;
import com.example.springbootApi.pojo.HBaseInsertPOJO;
import com.example.springbootApi.pojo.HBaseRowKeyPOJO;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * @Author grassPrince
 * @Date 2021/2/26 15:27
 * @Description 数据处理工具类
 **/
@Slf4j
public class DataHandleUtil {

    /**
     * 将文件处理成直接插入的数据格式，生成两个插入对象：上传文件内容表 和 上传文件配置表 (上传的表名及列族都是默认配置)
     * @param multipartFile 待上传文件
     * @param fileName 文件名
     * @return
     */
    public static List<HBaseInsertPOJO> file2UploadTable(MultipartFile multipartFile, String fileName) {
        String row = CommonUtil.getMD5(fileName);
        if (!CommonUtil.isNotEmpty(fileName)) {
            fileName = multipartFile.getOriginalFilename();
        }
        String[] fileSplit = fileName.split("[.]");
        String fileType = fileSplit.length > 1 ? fileSplit[fileSplit.length - 1] : "";


        // 生成 处理文件到上传内容表 的数据
        HBaseInsertPOJO content = new HBaseInsertPOJO();
        content.setTableName(TableName.valueOf(UploadTable.FILE_TABLE_NAME));
        List<HBaseRowKeyPOJO> contentRows = new ArrayList<>();
        content.setRows(contentRows);
        // 接收数据的对象，多次读取时覆盖上一次的读取内容
        byte[] buff = new byte[UploadTable.PIECE_SIZE];
        int lg = -1;
        try {
            // 读入文件流
            BufferedInputStream in = new BufferedInputStream(multipartFile.getInputStream());
            // 文件被切割的数量
            int fileNum = 0;
            // 一个文件块一个文件块地读StrMD5
            while ((lg = in.read(buff)) != -1) {
                byte[] value = new byte[lg];
                // 当读取长度达不到预定长度，多出的长度内容为上一次读取的内容，须去除
                if (lg != UploadTable.PIECE_SIZE) {
                    // 复制lg长度的内容到新临时数组
                    System.arraycopy(buff, 0, value, 0, lg);
                } else {
                    value = buff;
                }
                contentRows.add(
                        // 上传文件内容表的row为 md5(文件名) + 切割编号
                        new HBaseRowKeyPOJO(Bytes.toBytes(row + (++fileNum)),
                                Collections.singletonList(new HBaseCellPOJO(UploadTable.FILE_FAMILY.getBytes(), UploadTable.FILE_QUALIFIER.getBytes(), value))));
            }

            // 生成 处理文件到上传配置表 的数据
            HBaseInsertPOJO info = new HBaseInsertPOJO();
            info.setTableName(TableName.valueOf(UploadTable.INFO_TABLE_NAME));
            List<HBaseCellPOJO> infoCells = new ArrayList<>();
            infoCells.add(new HBaseCellPOJO(UploadTable.INFO_FAMILY.getBytes(), UploadTable.INFO_QUALIFIER[0].getBytes(), fileName.getBytes()));
            infoCells.add(new HBaseCellPOJO(UploadTable.INFO_FAMILY.getBytes(), UploadTable.INFO_QUALIFIER[1].getBytes(), fileType.getBytes()));
            infoCells.add(new HBaseCellPOJO(UploadTable.INFO_FAMILY.getBytes(), UploadTable.INFO_QUALIFIER[2].getBytes(), Bytes.toBytes(fileNum)));
            infoCells.add(new HBaseCellPOJO(UploadTable.INFO_FAMILY.getBytes(), UploadTable.INFO_QUALIFIER[3].getBytes(), Bytes.toBytes(multipartFile.getSize())));
            info.setRows(Collections.singletonList(new HBaseRowKeyPOJO(row.getBytes(), infoCells)));

            return Arrays.asList(content, info);
        } catch (IOException e) {
            log.error("处理文件成入表数据错误", e);
            return null;
        }
    }

}
