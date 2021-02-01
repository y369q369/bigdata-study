package com.example.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author grassPrince
 * @Date 2021/2/1 14:44
 * @Description TODO
 **/
public class WorkCount1 {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(WorkCount1.class);
        // Specify various job-specific parameters
        job.setJobName("myjob");

//        job.setInputPath(new Path("in"));
//        job.setOutputPath(new Path("out"));

        job.setMapperClass(WorkCount1.MyMapper.class);
        job.setReducerClass(WorkCount1.MyReducer.class);
        // Submit the job, then poll for progress until the job is complete
//        job.waitForCompletion(true);
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

    class MyReducer extends Reducer  {
        private IntWritable result = new IntWritable();
        public void reduce(Key key, Iterable&lt;IntWritable&gt; values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
