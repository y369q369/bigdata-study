package com.example.springbootApi.constant;

/**
 * @Author grassPrince
 * @Date 2021/2/26 15:03
 * @Description 上传文件的表信息：包含文件的信息表和文件内容表
 **/
public interface UploadTable {

    /** 存储上传文件内容的表名 */
    String FILE_TABLE_NAME = "uploadFileContent";

    /** 存储上传文件内容的表对应的列族 */
    String FILE_FAMILY = "content";

    /** 存储上传文件内容的表对应的列族元素，默认数据类型为string */
    String FILE_QUALIFIER = "detail";

    /** 存储上传文件信息的维度表名 */
    String INFO_TABLE_NAME = "uploadFileInfo";

    /** 存储上传文件信息的维度表的列族 */
    String INFO_FAMILY = "info";

    /** 存储上传文件信息的维度表的列族元素， rowSize表示上传文件被切分的数量，默认数据类型为string */
    String[] INFO_QUALIFIER = {"fileName", "type", "rowSize", "fileSize"};

    /** 文件每次切割的大小：100k */
    Integer PIECE_SIZE = 100 * 1024;

}
