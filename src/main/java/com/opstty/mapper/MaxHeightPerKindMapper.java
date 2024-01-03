package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightPerKindMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text treeKind = new Text();
    private DoubleWritable height = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(";");
        try {
            String kind = parts[3].trim();
            double treeHeight = Double.parseDouble(parts[7].trim());
            treeKind.set(kind);
            height.set(treeHeight);
            context.write(treeKind, height);
        } catch (NumberFormatException e) {}
    }
}

