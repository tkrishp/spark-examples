package com.tulc.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkExamples").setMaster("local[*]"));
        if (args.length == 0) {
            System.out.println("Missing spark application name.");
            System.out.println("Examples: [WordCount, Variance, Mean]");
            return;
        }
        else {
            if (args[0].equals("WordCount")) {
                WordCount wc = new WordCount(sc);
            }
        }
        

    }

}
