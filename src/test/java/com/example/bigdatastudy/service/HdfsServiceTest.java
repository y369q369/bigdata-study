package com.example.bigdatastudy.service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ExtendWith(SpringExtension.class)
class HdfsServiceTest {

    @Autowired
    private HdfsService hdfsService;

    @Test
    void getFileSystem() throws IOException {
        FileSystem fileSystem = hdfsService.getFileSystem();
        Assertions.assertNotNull(fileSystem);
    }

    @Test
    void getHomeDirectory() throws IOException {
        Path path = hdfsService.getHomeDirectory();
        Assertions.assertNotNull(path);
    }

    @Test
    void listStatus() throws IOException {
        hdfsService.listStatus();
    }



    @Test
    void mkdir() throws IOException {
        boolean mkdirFlag = hdfsService.mkdir("/test2");
        Assertions.assertTrue(mkdirFlag);
    }
}