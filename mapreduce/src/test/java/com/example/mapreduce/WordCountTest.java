package com.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class WordCountTest {

    @Test
    public void maxTemperatureMapper() throws IOException {
        Text value = new Text("6190557893565021");
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxTemperature.MaxTemperatureMapper())
                .withInput(new LongWritable(10),value)
                .withOutput(new Text("1905"),new IntWritable(7896))
                .runTest();
    }

    @Test
    public void maxTemperatureReducer() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer((Reducer<Text, IntWritable, Text, IntWritable>) new MaxTemperature.MaxTemperatureReducer())
                .withInput(new Text("1905"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("1905"),new IntWritable(7896))
                .runTest();
    }

    @Test
    public void use() throws Exception {
        System.setProperty("HADOOP_HOME", "F:\\Java\\hadoop-3.3.0");
        // 设置操作hadoop的用户，解决 org.apache.hadoop.security.AccessControlException: Permission denied: user=k
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://81.68.115.37:9000");
        configuration.set("mapreduce.framework.name", "local");
        configuration.setInt("mpreduce.task.io.sort.mb", 1);

        Path input = new Path("file/input.txt");
        Path output = new Path("file/output.txt");

        FileSystem fs = FileSystem.getLocal(configuration);
        fs.delete(output, true);

        MaxTemperature driver = new MaxTemperature();
        driver.setConf(configuration);

        int exitCode = ToolRunner.run(new MaxTemperature(), new String[]{input.toString(), output.toString()});
        System.out.println(exitCode);
    }

}