package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreeCountReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    private int maxTreeCount = Integer.MIN_VALUE;
    private Text maxDistrict = new Text();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            String district = parts[0];
            int treeCount = Integer.parseInt(parts[1]);

            if (treeCount > maxTreeCount) {
                maxTreeCount = treeCount;
                maxDistrict.set(district);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the district containing the most trees
        if (maxTreeCount != Integer.MIN_VALUE) {
            context.write(maxDistrict, new IntWritable(maxTreeCount));
        }
    }
}
