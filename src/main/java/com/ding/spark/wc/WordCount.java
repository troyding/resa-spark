package com.ding.spark.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by ding on 14-1-20.
 */
public class WordCount {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(args[0]).setMaster(args[1]);
        final Set<String> stopWord = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                WordCount.class.getResourceAsStream("/stop.txt")))) {
            reader.lines().filter(w -> !w.isEmpty()).forEach(stopWord::add);
        } catch (IOException e) {
        }
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> texts = context.textFile(args[2]);
        JavaRDD<String> words = texts.flatMap(sentence -> Arrays.asList(sentence.replaceAll("\\p{P}|\\p{S}", " ")
                .split("\\s+"))).map(String::trim).filter(w -> !w.isEmpty()).map(String::toLowerCase)
                .filter(w -> !stopWord.contains(w));
        List<Tuple2<String, Integer>> ret = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2)
                .takeOrdered(Integer.valueOf(args[3]), new TupleComparator());
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[4]))) {
            for (Tuple2<String, Integer> tuple2 : ret) {
                writer.append(tuple2._1());
                writer.newLine();
            }
        } catch (IOException e) {
        }
    }

    public static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return Integer.compare(o2._2(), o1._2());
        }
    }

}
