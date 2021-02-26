package com.example.springbootApi.controller;

import com.example.springbootApi.service.HBaseService;
import com.example.springbootApi.service.HdfsService;
import com.example.springbootApi.vo.ResultVO;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author grassPrince
 * @Date 2021/2/25 11:26
 * @Description HBase的api操作
 **/
@RestController
@RequestMapping("/hdfs")
@Api(tags = {"hdfs"})
public class HdfsController {

    @Autowired
    private HdfsService hdfsService;

    @GetMapping("/file/{FileName}")
    @ApiOperation(value = "file", notes = "新建文件")
    public ResultVO createFile(@PathVariable String FileName) {
        return null;
    }
}
