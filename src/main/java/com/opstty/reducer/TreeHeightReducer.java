package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TreeHeightReducer extends Reducer<DoubleWritable, NullWritable, NullWritable, Text> {

    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        // Emit each height in ascending order
        context.write(NullWritable.get(), new Text(String.valueOf(key.get())));
    }
}
