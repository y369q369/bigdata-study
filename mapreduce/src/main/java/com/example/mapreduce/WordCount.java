package com.example.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author grassPrince
 * @Date 2021/2/1 14:44
 * @Description TODO
 **/
public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        // Specify various job-specific parameters
        job.setJobName("myjob");

//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));

        // 设置数据输入源，可以有多个
        FileInputFormat.addInputPath(job, new Path("/test"));
        // 设置数据输出源，只能有一个
        Path outputPath = new Path("/test2");
        if (FileSystem.get(conf).exists(outputPath)) {
            FileSystem.get(conf).delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(WordCount.MyMapper.class);
        job.setReducerClass(WordCount.MyReducer.class);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }

    class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
