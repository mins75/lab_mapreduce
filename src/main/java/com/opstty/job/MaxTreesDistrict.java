package com.opstty.job;

import com.opstty.mapper.DistrictTreeCountMapper;
import com.opstty.reducer.DistrictTreeCountReducer;
import com.opstty.mapper.MaxTreeCountMapper;
import com.opstty.reducer.MaxTreeCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTreesDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "DistrictTreeCount");
        job1.setJarByClass(MaxTreesDistrict.class);
        job1.setMapperClass(DistrictTreeCountMapper.class);
        job1.setReducerClass(DistrictTreeCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "MaxTreeCount");
        job2.setJarByClass(MaxTreesDistrict.class);
        job2.setMapperClass(MaxTreeCountMapper.class);
        job2.setReducerClass(MaxTreeCountReducer.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/result"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
