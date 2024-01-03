package com.opstty.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import other.AgeDistrictWritable;

import java.io.IOException;

public class ReducerOldestTree extends Reducer<Text, AgeDistrictWritable, NullWritable, Text> {

    private int maxAge = 0;
    private Text oldestTreeDistrict = new Text();

    @Override
    protected void reduce(Text key, Iterable<AgeDistrictWritable> values, Context context) throws IOException, InterruptedException {
        for (AgeDistrictWritable ageDistrictWritable : values) {
            int age = ageDistrictWritable.getAge();
            if (age > maxAge) {
                maxAge = age;
                oldestTreeDistrict.set(ageDistrictWritable.getDistrict());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (maxAge > 0) {
            context.write(NullWritable.get(), oldestTreeDistrict);
        }
    }
}
