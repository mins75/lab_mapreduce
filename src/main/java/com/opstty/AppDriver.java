package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("wordcount_ex1", WordCount_ex1.class,
                    "A map/reduce program that counts the number of trees per district.\n");
            programDriver.addClass("allspecies_182", AllSpecies_182.class,
                    "A map/reduce program that returns all of the species and their number.\n");
            programDriver.addClass("maxheightperkind", MaxHeightPerKind.class,
                    "A map/reduce program that returns the max height per kind.\n");
            programDriver.addClass("treeheightsort", TreeHeightSort.class,
                    "A map/reduce program that returns the trees sorted by height.\n");
            programDriver.addClass("oldesttreeperdistrict", OldestTreePerDistrict.class,
                    "A map/reduce program that returns the district with the oldest tree.\n");
            programDriver.addClass("maxtreesdistrict", MaxTreesDistrict.class,
                    "A map/reduce program that returns the district with max number of trees.\n");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
