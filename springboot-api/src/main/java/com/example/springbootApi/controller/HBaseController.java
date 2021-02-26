package com.example.springbootApi.controller;

import com.example.springbootApi.constant.UploadTable;
import com.example.springbootApi.model.HBaseGetModel;
import com.example.springbootApi.po.HBaseTablePO;
import com.example.springbootApi.model.HBaseScanModel;
import com.example.springbootApi.model.HBaseTableModel;
import com.example.springbootApi.service.HBaseService;
import com.example.springbootApi.vo.ResultVO;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * @Author grassPrince
 * @Date 2021/2/25 11:26
 * @Description HBase的api操作
 **/
@RestController
@RequestMapping("HBase")
@Api(tags = {"HBase"})
public class HBaseController {

    @Autowired
    private HBaseService hBaseService;


    @PostMapping("table")
    @ApiOperation(value = "table", notes = "创建表")
    public ResultVO createTable(@RequestBody HBaseTableModel hBaseTableModel) {
        return hBaseService.createTable(hBaseTableModel);
    }

    @DeleteMapping("table/{tableName}")
    @ApiOperation(value = "table", notes = "删表")
    @ApiParam(name = "tableName", value = "表名", example = "test")
    public ResultVO deleteTable(@PathVariable String tableName) {
        return hBaseService.deleteTable(tableName);
    }

    @GetMapping("tables")
    @ApiOperation(value = "tables", notes = "获取所有表信息")
    public ResultVO getTableList() {
        return hBaseService.getTableList();
    }

    @PostMapping("insertData")
    @ApiOperation(value = "insertData", notes = "向表中插入数据")
    public ResultVO insertData(@RequestBody HBaseTablePO hBaseInsertPO) {
        return hBaseService.insertData(hBaseInsertPO.convert());
    }

    @PostMapping("scanData")
    @ApiOperation(value = "scanData", notes = "scan扫描表中数据 （查询参数过多，封装成post查询）")
    public ResultVO getDataByScan(@RequestBody HBaseScanModel hBaseScanModel) {
        return hBaseService.scanData(hBaseScanModel);
    }

    @PostMapping("getData")
    @ApiOperation(value = "getData", notes = "get获取表中数据 （查询参数过多，封装成post查询）")
    public ResultVO getDataByGet(@RequestBody HBaseGetModel hBaseGetModel) {
        return hBaseService.getData(hBaseGetModel);
    }

    @PostMapping("upload")
    @ApiOperation(value = "upload", notes = "将文件上传按固定大小拆分，分别存储到HBase中")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "multipartFile", value = "文件", dataType = "__File", required = true, allowMultiple = true),
            @ApiImplicitParam(name = "fileName", value = "文件名称", dataType = "string"),
    })
    public ResultVO upload(@RequestParam(value = "multipartFile") MultipartFile multipartFile,
                           @RequestParam(value = "fileName") String fileName) {
        //
        return hBaseService.upload(multipartFile, fileName);
    }

}
