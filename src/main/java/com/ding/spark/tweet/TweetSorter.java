package com.ding.spark.tweet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONValue;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Created by ding on 14-6-16.
 */
public class TweetSorter {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> texts = context.textFile(args[0]);
        texts.mapToPair(new TweetParser()).filter(Objects::nonNull)
                .sortByKey(new LongComparator(), true, Integer.parseInt(args[1]))
                .map(t -> t._1() + "@" + t._2().replace('\n', ' ')).saveAsTextFile(args[2]);
    }

    private static class LongComparator implements Comparator<Long>, Serializable {
        @Override
        public int compare(Long o1, Long o2) {
            return Long.compare(o1, o2);
        }
    }

    private static class TweetParser implements PairFunction<String, Long, String> {

        private static DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
        private LocalDateTime earliest = LocalDateTime.of(2011, Month.JANUARY, 1, 0, 1);

        @Override
        public Tuple2<Long, String> call(String s) throws Exception {
            Map<String, Object> data = (Map<String, Object>) JSONValue.parse(s);
            LocalDateTime time = LocalDateTime.parse((String) data.get("created_at"), formatter);
            return time.isBefore(earliest) ? null : new Tuple2<>(time.toEpochSecond(ZoneOffset.UTC),
                    (String) data.getOrDefault("text", ""));
        }
    }

}
