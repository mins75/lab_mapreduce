package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTreeCountMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private long maxTreeCount = Long.MIN_VALUE;
    private Text maxDistrict = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        String districtName = parts[0].trim();
        long treeCount = Long.parseLong(parts[1].trim());

        if (treeCount > maxTreeCount) {
            maxTreeCount = treeCount;
            maxDistrict.set(districtName);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the district containing the most trees
        if (maxTreeCount != Long.MIN_VALUE) {
            context.write(NullWritable.get(), new Text(maxDistrict + "\t" + maxTreeCount));
        }
    }
}
