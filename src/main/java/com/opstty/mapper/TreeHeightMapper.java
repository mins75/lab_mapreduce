package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeHeightMapper extends Mapper<Object, Text, DoubleWritable, NullWritable> {

    private DoubleWritable height = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(";");
        try {
            //String kind = parts[3].trim();
            double treeHeight = Double.parseDouble(parts[7].trim());
            height.set(treeHeight);
            context.write(height, NullWritable.get());
        } catch (NumberFormatException e) {}
    }
}
