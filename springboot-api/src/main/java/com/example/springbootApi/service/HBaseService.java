package com.example.springbootApi.service;

import com.example.springbootApi.constant.UploadTable;
import com.example.springbootApi.model.*;
import com.example.springbootApi.po.HBaseCellPO;
import com.example.springbootApi.po.HBaseRowKeyPO;
import com.example.springbootApi.po.HBaseTablePO;
import com.example.springbootApi.pojo.HBaseCellPOJO;
import com.example.springbootApi.pojo.HBaseInsertPOJO;
import com.example.springbootApi.pojo.HBaseRowKeyPOJO;
import com.example.springbootApi.utils.CommonUtil;
import com.example.springbootApi.utils.DataHandleUtil;
import com.example.springbootApi.vo.HBaseTableVO;
import com.example.springbootApi.vo.ResultVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;

/**
 * @Author grassPrince
 * @Date 2021/2/7 16:12
 * @Description HBASE的操作业务类
 *                  参考文档：https://blog.csdn.net/m0_38075425/article/details/81287836
 *                           https://blog.csdn.net/m0_38075425/article/details/81287836
 **/
@Service
@Slf4j
public class HBaseService {

    public Configuration getConfiguration(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "120.26.184.85");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        return configuration;
    }

    /**
     * 创建Hbase连接
     * @return
     * @throws IOException
     */
    public Connection getConnection(){
        Configuration configuration = getConfiguration();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            log.error("创建HBase连接失败", e);
        }
        return connection;
    }

    public Admin getAdmin(){
        Connection connection = getConnection();
        Admin admin = null;
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            log.error("创建HBaseAdmin失败", e);
        }
        return admin;
    }



    /**
     * 判断表是否存在
     * @param table 表名
     * @return 判断结果
     */
    public boolean isTableExist(String table) {
        Admin admin = getAdmin();
        try {
            boolean flag = admin.tableExists(TableName.valueOf(table));
            return flag;
        } catch (IOException e) {
            log.error("判断表 {} 是否存在失败", table, e);
            return false;
        }
    }


    /**
     * 创建表
     * @param table 表对象
     */
    public ResultVO createTable(HBaseTableModel table) {
        // 获取操作对象
        Admin admin = getAdmin();
        // 构建一个test2表
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(table.getName()));
        // 创建列族
        for (String cf : table.getFamilies()) {
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(cf);
            tableDescriptorBuilder.setColumnFamily(cfd);
        }
        // 构建
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        // 创建表
        try {
            admin.createTable(tableDescriptor);
            log.info("建表 {} 成功", table.getName());
            return ResultVO.success("建表 " + table.getName() + " 成功");
        } catch (IOException e) {
            log.error("建表 {} 失败", table.getName(), e);
            return ResultVO.fail("建表 " + table.getName() + " 失败, 错误信息： " + e.getMessage());
        }

    }

    /**
     * 强制创建表：判断表是否存在 -> 不存在在时建表
     * @param table 表对象
     */
    public ResultVO createCompulsionTable(HBaseTableModel table) {
        Admin admin = getAdmin();
        try {
            if (!admin.tableExists(TableName.valueOf(table.getName()))) {
                return createTable(table);
            };
            return ResultVO.success("表 " + table.getName() + " 已存在， 不需重建");
        } catch (IOException e) {
            log.error("判断表 {} 是否存在失败", table, e);
            return ResultVO.fail("判断表 " + table.getName() + " 是否存在失败, 错误信息： " + e.getMessage());
        }
    }

    /**
     * 删除表
     * @param table 表名
     */
    public ResultVO deleteTable(String table) {
        Admin admin = getAdmin();
        try {
            boolean flag = admin.tableExists(TableName.valueOf(table));
            if (flag) {
                admin.disableTable(TableName.valueOf(table));
                admin.deleteTable(TableName.valueOf(table));
                return ResultVO.success("删表 " + table + " 成功");
            }
            return ResultVO.fail("表 " + table + " 不存在");
        } catch (IOException e) {
            log.error("删除表 {} 失败", table, e);
            return ResultVO.fail("删除表 " + table + " 失败, 错误信息： " + e.getMessage());
        }
    }

    /**
     * 获取所有表
     */
    public ResultVO getTableList() {
        Admin admin = getAdmin();
        List<HBaseTableVO> tables = new ArrayList<>();

        List<TableDescriptor> list = null;
        try {
            list = admin.listTableDescriptors();
            for (TableDescriptor tableDescriptor: list) {
                tables.add(new HBaseTableVO(tableDescriptor.getTableName().getNameAsString(), tableDescriptor.getColumnFamilies()));
            }
            return ResultVO.success(tables);
        } catch (IOException e) {
            log.error("获取所有表信息失败", e);
            return ResultVO.fail("获取所有表信息失败, 错误信息： " + e.getMessage());
        }

    }

    /**
     * 插入数据
     * @param data 数据对象
     */
    public ResultVO insertData(HBaseInsertPOJO data) {
        Connection connection = getConnection();
        List<Put> puts = new ArrayList<>();
        for (HBaseRowKeyPOJO rowData : data.getRows()) {
            Put put = new Put(rowData.getRow());
            for (HBaseCellPOJO column : rowData.getCells()) {
                //参数：1.列族名  2.列名  3.值
                put.addColumn(column.getFamily(), column.getQualifier(), column.getValue()) ;
            }
            puts.add(put);
        }
        try {
            Table table = connection.getTable(data.getTableName());
            table.put(puts);
            return ResultVO.success("数据插入成功");
        } catch (IOException e) {
            log.error("插入 {} 数据失败", data.getTableName().getNameAsString(), e);
            return ResultVO.fail("数据插入失败, 错误信息： " + e.getMessage());
        }
    }

    /**
     * 使用scan扫描表里的数据
     * @param hBaseScanModel 扫描参数对象
     */
    public ResultVO scanData(HBaseScanModel hBaseScanModel) {
        Connection connection = getConnection();
        Scan scan = new Scan();
        // 设置扫描的起始row
        if(CommonUtil.isNotEmpty(hBaseScanModel.getStartRow())) {
            scan.withStartRow(hBaseScanModel.getStartRow().getBytes());
        }
        // 设置扫描的结尾row
        if(CommonUtil.isNotEmpty(hBaseScanModel.getStopRow())) {
            scan.withStopRow(hBaseScanModel.getStopRow().getBytes());
        }
        // 设置扫描的列族和元素
        if(CommonUtil.isNotEmpty(hBaseScanModel.getCells())) {
            for (HBaseScanCellModel cellModel : hBaseScanModel.getCells()) {
                if (CommonUtil.isNotEmpty(cellModel.getFamily())) {
                    if (CommonUtil.isNotEmpty(cellModel.getQualifier())) {
                        scan.addColumn(cellModel.getFamily().getBytes(), cellModel.getQualifier().getBytes());
                    } else {
                        scan.addFamily(cellModel.getFamily().getBytes());
                    }
                }
            }
        }
        try {
            Table table = connection.getTable(TableName.valueOf(hBaseScanModel.getName()));
            ResultScanner results = table.getScanner(scan);
            return handleResult(hBaseScanModel.getName(), results);
        } catch (IOException e) {
            log.error("scan扫描 {} 数据失败", hBaseScanModel.getName(), e);
            return ResultVO.fail("scan扫描数据失败, 错误信息： " + e.getMessage());
        }
    }

    /**
     * 使用get获取表里指定row的数据
     * @param hBaseGetModel get获取参数对象
     */
    public ResultVO getData(HBaseGetModel hBaseGetModel) {
        Connection connection = getConnection();
        List<Get> gets = new ArrayList<>();
        if(CommonUtil.isNotEmpty(hBaseGetModel.getCells())) {
            for (HBaseGetCellModel cellModel : hBaseGetModel.getCells()) {
                if (CommonUtil.isNotEmpty(cellModel.getRow())) {
                    // 设置获取的row
                    Get get = new Get(cellModel.getRow().getBytes());
                    // 设置获取row的列族和元素
                    if (CommonUtil.isNotEmpty(cellModel.getFamily())) {
                        if (CommonUtil.isNotEmpty(cellModel.getQualifier())) {
                            get.addColumn(cellModel.getFamily().getBytes(), cellModel.getQualifier().getBytes());
                        } else {
                            get.addFamily(cellModel.getFamily().getBytes());
                        }
                    }
                    gets.add(get);
                }
            }
        }
        try {
            Table table = connection.getTable(TableName.valueOf(hBaseGetModel.getName()));
            Result[] results = table.get(gets);
            return handleResult(hBaseGetModel.getName(), Arrays.asList(results));
        } catch (IOException e) {
            log.error("get获取 {} 数据失败", hBaseGetModel.getName(), e);
            return ResultVO.fail("get获取数据失败, 错误信息： " + e.getMessage());
        }
    }

    /**
     * 处理获取结果
     * @param table 表名
     * @param results 查询的数据集合
     * @return
     */
    public ResultVO handleResult(String table, Iterable<Result> results) {
        HBaseTablePO tablePO = new HBaseTablePO();
        tablePO.setTableName(table);
        List<HBaseRowKeyPO> rows = new ArrayList<>();
        tablePO.setRows(rows);
        for (Result result: results) {
            HBaseRowKeyPO rowKeyPO = new HBaseRowKeyPO();
            rowKeyPO.setRow(Bytes.toString(result.getRow()));
            List<HBaseCellPO> cells = new ArrayList<>();
            rowKeyPO.setCells(cells);
            for(Cell cell : result.listCells()){
                HBaseCellPO hBaseCellPO = new HBaseCellPO();
                hBaseCellPO.setFamily(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                hBaseCellPO.setQualifier(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                hBaseCellPO.setValue(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                cells.add(hBaseCellPO);
            }
            rows.add(rowKeyPO);
        }
        return ResultVO.success(tablePO);
    }

    /**
     * 上传文件到HBase表，将文件切割成固定大小的内容多次上传
     * @param multipartFile 待上传文件
     * @param fileName 文件名
     * @return
     */
    public ResultVO upload(MultipartFile multipartFile, String fileName) {
        ResultVO resultVO = createCompulsionTable(new HBaseTableModel(UploadTable.FILE_TABLE_NAME, Collections.singletonList(UploadTable.FILE_FAMILY)));
        if (resultVO.isStatus()) {
            resultVO = createCompulsionTable(new HBaseTableModel(UploadTable.INFO_TABLE_NAME, Collections.singletonList(UploadTable.INFO_FAMILY)));
            if(resultVO.isStatus()) {
                List<HBaseInsertPOJO> list = DataHandleUtil.file2UploadTable(multipartFile, fileName);
                if(list != null) {
                    for (HBaseInsertPOJO insert: list) {
                        resultVO = insertData(insert);
                        if (!resultVO.isStatus()) {
                            // TODO 此处未处理文件内容插入成功，配置信息未插入成功的事务
                            return ResultVO.success("文件上传成功");
                        }
                    }
                } else {
                    resultVO = ResultVO.fail("文件处理过程失败，尚未传入表");
                }
            }
        }

        return resultVO;
    }

    /**
     * 从上传文件信息表中获取文件信息
     * @param fileName 文件名
     * @param fileSize 文件切割数量
     * @return
     */
    public List<HBaseRowKeyPO> getContent(String fileName, Integer fileSize) {
        String row = CommonUtil.getMD5(fileName);
        HBaseGetModel hBaseGetModel = new HBaseGetModel();
        hBaseGetModel.setName(UploadTable.FILE_TABLE_NAME);
        List<HBaseGetCellModel> cellModels = new ArrayList<>();
        hBaseGetModel.setCells(cellModels);
        for(int i = 1; i <= fileSize; i++) {
            HBaseGetCellModel getCellModel = new HBaseGetCellModel();
            getCellModel.setRow(row + i);
            getCellModel.setFamily(UploadTable.FILE_FAMILY);
            getCellModel.setQualifier(UploadTable.FILE_QUALIFIER);
            cellModels.add(getCellModel);
        }
        ResultVO resultVO = getData(hBaseGetModel);
        if(resultVO.isStatus()) {
            HBaseTablePO tablePO = (HBaseTablePO) resultVO.getMessage();
            if(tablePO.getRows().size() > 0) {
                return tablePO.getRows();
            }
        }
        return null;
    }

    /**
     * 从上传文件信息表中获取文件信息
     * @param fileName 文件名
     * @return
     */
    public HBaseRowKeyPO getInfo(String fileName) {
        HBaseGetModel hBaseGetModel = new HBaseGetModel();
        hBaseGetModel.setName(UploadTable.INFO_TABLE_NAME);
        HBaseGetCellModel getCellModel = new HBaseGetCellModel();
        getCellModel.setRow(CommonUtil.getMD5(fileName));
        hBaseGetModel.setCells(Collections.singletonList(getCellModel));
        ResultVO resultVO = getData(hBaseGetModel);
        if(resultVO.isStatus()) {
            HBaseTablePO tablePO = (HBaseTablePO) resultVO.getMessage();
            if(tablePO.getRows().size() > 0) {
                return tablePO.getRows().get(0);
            }
        }
        return null;
    }


    /**
     * 从hdfs中将上传的文件 下载到本地
     * @param fileName 文件名
     * @throws IOException 异常
     */
    public ResultVO download(String fileName, HttpServletResponse response) {
        HBaseRowKeyPO hBaseRowKeyPO = getInfo(fileName);
        if (hBaseRowKeyPO != null) {
            // 设置强制下载不打开
            response.setContentType("application/force-download");
            // 设置文件名
            response.addHeader("Content-Disposition", "attachment;fileName=" + fileName);
            Integer fileSize = 0;
            for (HBaseCellPO cellPO : hBaseRowKeyPO.getCells()) {
                if(UploadTable.INFO_QUALIFIER[2].equals(cellPO.getQualifier())) {
                    fileSize = Integer.valueOf(cellPO.getValue());
                }
            }

            try {
                OutputStream os = response.getOutputStream();
                List<HBaseRowKeyPO> list = getContent(fileName, fileSize);
                for (HBaseRowKeyPO rowKeyPO : list) {
                    for (HBaseCellPO cellPO : rowKeyPO.getCells()) {
                        log.info(cellPO.getValue());
                        os.write(cellPO.getValue().getBytes());
                    }
                }
                return ResultVO.success("下载成功");
            } catch (IOException e) {
                log.error("下载失败", e);
                return ResultVO.fail("下载失败, 错误信息： " + e.getMessage());
            }
        } else {
            return ResultVO.fail("文件不存在");
        }
    }


    public static void main(String[] args) throws IOException {
        HBaseService hbaseService = new HBaseService();

//        hbaseService.createTable("test3", "info", "base", "emp", "advance");

//        hbaseService.deleteTable("test3");

        hbaseService.getTableList();

//        String[] qualifiers = {"base", "emp", "advance"};
//        List<Map<String, Object>> columns = new ArrayList<>();
//        for (int i = 1; i < 4; i++) {
//            Map<String, Object> column = new HashMap<>();
//            column.put("row", String.valueOf(new Random().nextInt(1000000)));
//            List<Map<String, String>> list = new ArrayList<>();
//            column.put("cell", list);
//            for( String qualifier : qualifiers) {
//                Map<String, String> cell = new HashMap<>();
//                cell.put("family", "info");
//                cell.put("qualifier", qualifier);
//                cell.put("value", String.valueOf(new Random().nextInt(1000000)));
//                list.add(cell);
//            }
//            columns.add(column);
//        }
//        hbaseService.insertData2("test3", columns);

//        hbaseService.scanData("test3");

//        hbaseService.getData("test3", Arrays.asList("115321", "e9e868406875100a4d72322475d6d8eb2"));

//        hbaseService.upload("C:\\Users\\grassprince\\Desktop\\src2\\main\\java\\io\\transwarp\\config\\Test.java");

//        hbaseService.download("./test.txt", "test3",
//                Arrays.asList("e9e868406875100a4d72322475d6d8eb1", "e9e868406875100a4d72322475d6d8eb2", "e9e868406875100a4d72322475d6d8eb3"), "base");
    }


}
