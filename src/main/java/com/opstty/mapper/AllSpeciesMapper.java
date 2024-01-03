package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class AllSpeciesMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text district = new Text();

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");

        while (lines.hasMoreTokens()) {
            StringTokenizer itr = new StringTokenizer(lines.nextToken(), ";");

            if (itr.hasMoreTokens()) {
                //String districtValue = itr.nextToken();
                for (int i = 0; i < 3; i++) {
                    itr.nextToken();
                }
                district.set(itr.nextToken());
                context.write(district, one);
            }
        }
    }
}
