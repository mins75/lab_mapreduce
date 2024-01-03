package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper_ex1 extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text district = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");

        while (lines.hasMoreTokens()) {
            StringTokenizer itr = new StringTokenizer(lines.nextToken(), ";");
            if (itr.hasMoreTokens()) {
                itr.nextToken(); //get to arrondissement
                district.set(itr.nextToken());
                context.write(district, one);
            }
        }
    }
}
