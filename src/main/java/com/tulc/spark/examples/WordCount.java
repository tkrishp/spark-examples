package com.tulc.spark.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {
    
    public JavaSparkContext sc;
    
    public WordCount(JavaSparkContext jsc) {
        sc = jsc;
    }
    
    public void createRDD() {
        JavaRDD<String> rdd = sc.textFile("hdfs://data/input/words.txt");
    }
   
}