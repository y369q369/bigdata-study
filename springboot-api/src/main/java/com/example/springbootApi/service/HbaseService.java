package com.example.springbootApi.service;

import com.example.springbootApi.utils.CommonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

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
public class HbaseService {

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
     * 获取所有表
     */
    public List<TableDescriptor> getTableList() throws IOException {
        Admin admin = getAdmin();
        List<TableDescriptor> list = admin.listTableDescriptors();
        for (TableDescriptor tableDescriptor: list) {
            log.info("table: {}, columns: {}", tableDescriptor.getTableName(), Arrays.toString(tableDescriptor.getColumnFamilies()));
        }
        return list;
    }

    /**
     * 判断表是否存在
     * @param table 表名
     * @return 判断结果
     * @throws IOException
     */
    public boolean isTableExist(String table) throws IOException {
        Admin admin = getAdmin();
        boolean flag = admin.tableExists(TableName.valueOf(table));
        log.info("表 {} {}", table, flag ? "存在" : "不存在");
        return flag;
    }


    /**
     * 创建表
     * @param table 表名
     * @param columnFamily 列族名称
     * @throws IOException
     */
    public void createTable(String table, String ... columnFamily) throws IOException {
        // 获取操作对象
        Admin admin = getAdmin();
        // 构建一个test2表
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(table));
        // 创建列族
        for (String cf : columnFamily) {
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.of(cf);
            tableDescriptorBuilder.setColumnFamily(cfd);
        }
        // 构建
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        // 创建表
        admin.createTable(tableDescriptor);
        log.info("建表 {} 成功", table);
    }

    /**
     * 删除表
     * @param table 表名
     * @throws IOException
     */
    public void deleteTable(String table) throws IOException {
        Admin admin = getAdmin();
        admin.deleteTable(TableName.valueOf(table));
        log.info("删表 {} 成功", table);
    }

    /**
     * 插入数据
     * @param tableName 表名
     * @param columns 插入的数据
     * @throws IOException
     */
    public void insertData(String tableName, List<Map<String, Object>> columns) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();
        for (Map<String, Object> column : columns) {
            Put put = new Put(column.get("row").toString().getBytes());
//            List<Map<String, String>> cells = (List<Map<String, String>>) column.get("cell");
            for (Map<String, String> cell : (List<Map<String, String>>) column.get("cell")) {
                //参数：1.列族名  2.列名  3.值
                put.addColumn(cell.get("family").getBytes(), cell.get("qualifier").getBytes(), cell.get("value").getBytes()) ;
            }
            puts.add(put);
        }
        table.put(puts);
        log.info("表 {} 数据插入成功！", tableName);
    }

    /**
     * 使用scan扫描表里的数据
     * @param tableName 表名
     * @throws IOException
     */
    public List<Map<String, Object>> scanData(String tableName) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Map<String, Object>> columns = new ArrayList<>();
        Scan scan = new Scan();
        // 设置扫描的起始row
//        scan.withStartRow("e9e868406875100a4d72322475d6d8eb1".getBytes());
//        scan.withStopRow("e9e868406875100a4d72322475d6d8eb99".getBytes());
        ResultScanner results = table.getScanner(scan);
        // 使用 family 和 qualifier 进行扫描
//        ResultScanner results = table.getScanner("info".getBytes(), "advance".getBytes());
        for (Result result : results){
            Map<String, Object> column = new HashMap<>();
            column.put("row", new String(result.getRow()));
            List<Map<String, String>> cells = new ArrayList<>();
            column.put("cell", cells);
            for(Cell cell : result.listCells()){
                Map<String, String> realCell = new HashMap<>();
                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                realCell.put("family", family);
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                realCell.put("qualifier", qualifier);
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                realCell.put("value", value);
                cells.add(realCell);
            }
            columns.add(column);

        }
        log.info("scan获取表 {} 的数据： {}", tableName, columns.toString());
        return columns;
    }

    /**
     * 使用get获取表里指定row的数据
     * @param tableName 表名
     * @param rows row集合
     * @throws IOException
     */
    public List<Map<String, Object>> getData(String tableName, List<String> rows) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Map<String, Object>> columns = new ArrayList<>();

        for (String row : rows) {
            Get get = new Get(row.getBytes());
            // 设置获取的family
//            get.addFamily(Bytes.toBytes("base"));
            // 设置获取的family 和 qualifier
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("base"));

            Result result = table.get(get);
            Map<String, Object> column = new HashMap<>();
            column.put("row", new String(result.getRow()));
            List<Map<String, String>> cells = new ArrayList<>();
            column.put("cell", cells);
            for(Cell cell : result.listCells()){
                Map<String, String> realCell = new HashMap<>();
                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                realCell.put("family", family);
                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                realCell.put("qualifier", qualifier);
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                realCell.put("value", value);
                cells.add(realCell);
            }
            columns.add(column);
        }

        log.info("get获取表 {} 的数据： {}", tableName, columns.toString());
        return columns;
    }

    /**
     * 上传文件到hbase(将文件分割成固定大小的文本，多次存储到某个表中)
     * @param filePath 文件路径
     * @throws IOException
     */
    public void upload(String filePath) throws IOException {
        File file = new File(filePath);
        int size = 1024;
        // 接收数据的对象，多次读取时覆盖上一次的读取内容
        byte[] buff = new byte[size];
        int lg = -1;
        // 读入文件流
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));

        int i = 0;
        // 一个文件块一个文件块地读StrMD5
        while ((lg = in.read(buff)) != -1) {
            byte[] value = new byte[lg];
            // 当读取长度达不到预定长度，多出的长度内容为上一次读取的内容，须去除
            if (lg != size) {
                // 复制lg长度的内容到新临时数组
                System.arraycopy(buff, 0, value, 0, lg);
            } else {
                value = buff;
            }

            List<Map<String, Object>> columns = new ArrayList<>();
            Map<String, Object> column = new HashMap<>();
            column.put("row", CommonUtil.getMD5(filePath) + (++i));
            List<Map<String, String>> list = new ArrayList<>();
            column.put("cell", list);
            Map<String, String> cell = new HashMap<>();
            cell.put("family", "info");
            cell.put("qualifier", "base");
            cell.put("value", Bytes.toString(value));
            list.add(cell);
            columns.add(column);
            insertData("test3", columns);
        }
    }

    /**
     * 从hdfs中将上传的数据 下载到本地
     * @param filePath 下载路径
     * @param tableName 表名
     * @param rows row集合
     * @param qualifier 需要获取的列名
     * @throws IOException
     */
    public void download(String filePath, String tableName, List<String> rows, String qualifier) throws IOException {
        FileOutputStream fs = new FileOutputStream(filePath, true);
        List<Map<String, Object>> results = getData(tableName, rows);
        for (Map<String, Object> result : results) {
            List<Map<String, String>> cell = (List<Map<String, String>>) result.get("cell");
            for (Map<String, String> column : cell) {
                if (qualifier.equals(column.get("qualifier"))) {
                    log.info(column.get("value"));
                    fs.write(Bytes.toBytes(column.get("value")));
                }
            }
        }
    }


    public static void main(String[] args) throws IOException {
        HbaseService hbaseService = new HbaseService();

//        hbaseService.createTable("test3", "info", "base", "emp", "advance");

//        hbaseService.deleteTable("test3");

//        hbaseService.getTableList();

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
//        hbaseService.insertData("test3", columns);

//        hbaseService.scanData("test3");

//        hbaseService.getData("test3", Arrays.asList("115321", "e9e868406875100a4d72322475d6d8eb2"));

//        hbaseService.upload("C:\\Users\\grassprince\\Desktop\\src2\\main\\java\\io\\transwarp\\config\\Test.java");

        hbaseService.download("./test.txt", "test3",
                Arrays.asList("e9e868406875100a4d72322475d6d8eb1", "e9e868406875100a4d72322475d6d8eb2", "e9e868406875100a4d72322475d6d8eb3"), "base");
    }


}
