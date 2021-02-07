package com.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Author grassPrince
 * @Date 2021/2/3 15:59
 * @Description 天气
 **/
public class MaxTemperature extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_HOME", "F:\\Java\\hadoop-3.3.0");
        // 设置操作hadoop的用户，解决 org.apache.hadoop.security.AccessControlException: Permission denied: user=k
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://81.68.115.37:9000");
        configuration.set("mapreduce.framework.name", "local");
        configuration.setInt("mpreduce.task.io.sort.mb", 1);

        Path input = new Path("test/input");
        Path output = new Path("test/output");

        FileSystem fs = FileSystem.getLocal(configuration);
        fs.delete(output, true);

        MaxTemperature driver = new MaxTemperature();
        driver.setConf(configuration);

        int exitCode = ToolRunner.run(new MaxTemperature(), new String[]{input.toString(), output.toString()});
        System.out.println(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output> \n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Max temperature");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String year = line.substring(1, 5);
            int airTemperature = Integer.parseInt(line.substring(9, 11));
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }

    static class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
            }
            context.write(key, new IntWritable(maxValue));
        }
    }

}
