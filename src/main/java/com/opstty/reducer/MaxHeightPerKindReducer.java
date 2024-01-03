package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightPerKindReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double maxHeight = Double.MIN_VALUE;
        for (DoubleWritable val : values) {
            maxHeight = Math.max(maxHeight, val.get());
        }
        result.set(maxHeight);
        context.write(key, result);
    }
}
