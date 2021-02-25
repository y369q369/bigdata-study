package com.example.springbootApi.controller;

import com.example.springbootApi.model.HBaseTableModel;
import com.example.springbootApi.service.HBaseService;
import com.example.springbootApi.vo.ResultVO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @Author grassPrince
 * @Date 2021/2/25 11:26
 * @Description HBase的api操作
 **/
@RestController
@RequestMapping("/HBase")
@Api(tags = {"HBase"})
public class HBaseController {

    @Autowired
    private HBaseService hBaseService;


    @PostMapping("table")
    @ApiOperation(value = "table", notes = "创建表")
    public ResultVO createTable(@RequestBody HBaseTableModel hBaseTableModel) {
        return hBaseService.createTable(hBaseTableModel);
    }

    @DeleteMapping("/table/{tableName}")
    @Operation(summary = "table", description = "删表")
    public ResultVO deleteTable(@Parameter(description = "表名") @PathVariable String tableName) {
        return hBaseService.deleteTable(tableName);
    }

    @GetMapping("/tables")
    @Operation(summary = "tables", description = "获取所有表信息")
    public ResultVO getTableList() {
        return hBaseService.getTableList();
    }
}
