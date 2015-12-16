package com.tulc.spark.examples;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class SparkDriver {
    public static JavaSparkContext sc = null;
    public static void main(String[] args) {
        sc = new JavaSparkContext(new SparkConf().setAppName("SparkExamples").setMaster("local[*]"));
        if (args.length == 0) {
            System.out.println("Missing spark application name.");
            System.out.println("Examples: [WordCount, Variance, Mean]");
            return;
        }
        else {
            if (args[0].equals("WordCount")) {
                wordCount();
            }
        }
    }
    
    public static void wordCount() {
        JavaRDD<String> lines = sc.textFile("hdfs://trinity/data/input.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String line) {
                        return Arrays.asList(line.split(" "));
                    } 
                });
        Map<String, Long> hm = words.countByValue();
        for (String word : hm.keySet()) 
            System.out.println(word + "," + hm.get(word));
        System.out.println("Total words: " + words.count());
    }
}
