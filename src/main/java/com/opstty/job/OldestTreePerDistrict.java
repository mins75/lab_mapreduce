package com.opstty.job;

import com.opstty.mapper.MapperOldestTree;
import com.opstty.reducer.ReducerOldestTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import other.AgeDistrictWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OldestTreePerDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: oldesttreeperdistrict <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "OldestTreePerDistrict");
        job.setJarByClass(OldestTreePerDistrict.class);
        job.setMapperClass(MapperOldestTree.class);
        job.setReducerClass(ReducerOldestTree.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AgeDistrictWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
