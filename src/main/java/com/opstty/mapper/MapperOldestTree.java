package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import other.AgeDistrictWritable; // Import the custom Writable class

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperOldestTree extends Mapper<Object, Text, Text, AgeDistrictWritable> {
    private Text district = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Split the input into lines
        StringTokenizer lines = new StringTokenizer(value.toString(), "\n");

        while (lines.hasMoreTokens()) {
            StringTokenizer itr = new StringTokenizer(lines.nextToken(), ";");

            // Skip the first token (GEOPOINT)
            itr.nextToken();

            // Extract the district value (2nd column)
            String districtValue = itr.nextToken();

            // Skip the next three columns
            for (int i = 0; i < 3; i++) {
                itr.nextToken();
            }

            // Extract the year planted value (6th column)
            String yearPlantedStr = itr.nextToken();

            // Check if the yearPlantedStr is not empty or blank
            if (!yearPlantedStr.isEmpty()) {
                try {
                    int yearPlanted = Integer.parseInt(yearPlantedStr);
                    int currentYear = 2023; // Replace with the actual current year
                    int age = currentYear - yearPlanted;

                    district.set(districtValue);
                    AgeDistrictWritable ageDistrictWritable = new AgeDistrictWritable(age, districtValue);

                    context.write(district, ageDistrictWritable);
                } catch (NumberFormatException e) {
                    // Handle cases where the yearPlantedStr is not a valid integer (optional)
                    // You can choose to log an error or skip the record, or handle as appropriate.
                }
            }
        }
    }
}
