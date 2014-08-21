package com.ding.spark.taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;

/**
 * Created by ding on 14-6-16.
 */
public class RecordSorter {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> texts = context.textFile(args[0]);
        Comparator<Long> comparator = (Comparator & Serializable) (v1, v2) -> Long.compare((Long) v1, (Long) v2);
        texts.mapToPair(new RecordParser()).filter(Objects::nonNull).persist(StorageLevel.MEMORY_AND_DISK_SER())
                .sortByKey(comparator, true, Integer.parseInt(args[1])).persist(StorageLevel.MEMORY_AND_DISK_SER())
                .map(Tuple2::_2).saveAsTextFile(args[2]);
    }

    private static class RecordParser implements PairFunction<String, Long, String> {
        private static DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss", Locale.ENGLISH);

        @Override
        public Tuple2<Long, String> call(String s) throws Exception {
            try {
                LocalDateTime time = LocalDateTime.parse(s.split(",")[1], formatter);
                return new Tuple2<>(time.toEpochSecond(ZoneOffset.UTC), s);
            } catch (Exception e) {
                System.out.println("Bad record: " + s);
            }
            return null;
        }
    }

}
