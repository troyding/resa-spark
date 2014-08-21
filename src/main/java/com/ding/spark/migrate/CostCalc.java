package com.ding.spark.migrate;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-8-19.
 */
public class CostCalc {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        JavaSparkContext context = new JavaSparkContext(conf);
        double[] dataSizes = Files.readAllLines(Paths.get(args[0])).stream().map(String::trim)
                .filter(s -> !s.isEmpty()).mapToDouble(Double::parseDouble).toArray();
        TreeMap<String, Double> migrationMetrics = Files.readAllLines(Paths.get(args[1])).stream()
                .map(s -> s.split(":")).collect(Collectors.toMap(strs -> strs[0] + "-" + strs[1],
                        strs -> Double.parseDouble(strs[2]), (v1, v2) -> {
                            throw new IllegalStateException();
                        }, TreeMap::new));
        int[] states = migrationMetrics.keySet().stream().flatMap(s -> Stream.of(s.split("-"))).distinct()
                .mapToInt(Integer::parseInt).sorted().toArray();
        Map<Integer, JavaPairRDD<Long, int[]>> state2RDD = new HashMap<>();
        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(args[2]);
        for (int i = 0; i < states.length; i++) {
            JobConf hadoopConf = new JobConf();
            FileInputFormat.setInputPaths(hadoopConf, new org.apache.hadoop.fs.Path(hdfsPath, "state_" + states[i]));
            JavaPairRDD<Long, int[]> rdd = context.hadoopRDD(hadoopConf, TextInputFormat.class, LongWritable.class,
                    Text.class, 32).mapToPair(t -> new Tuple2<>((Long) t._1().get(), parse(t._2().toString(), ",")));
            state2RDD.put(states[i], rdd);
        }
        List<int[]> migrations = migrationMetrics.keySet().stream().map(s -> s.split("-"))
                .map(s -> new int[]{Integer.parseInt(s[0]), Integer.parseInt(s[0])}).collect(Collectors.toList());
//        org.apache.hadoop.fs.Path out = new org.apache.hadoop.fs.Path(args[3]);
        for (int[] m : migrations) {
//            try (CostWriter costWriter = new CostWriter(new DataOutputStream(new BufferedOutputStream(
//                    Files.newOutputStream(out.resolve("cost_" + m[0] + "-" + m[1])), 5 * 1024 * 1024)))) {
            KuhnMunkres km = new KuhnMunkres(dataSizes.length);
            JobConf hadoopConf = new JobConf();
            state2RDD.get(m[0]).cartesian(state2RDD.get(m[1]))
                    .mapToPair(t -> calcCost(t._1(), t._2(), km, dataSizes)).persist(StorageLevel.DISK_ONLY())
                    .sortByKey(new LongComparator(), true, 1024).persist(StorageLevel.DISK_ONLY())
                    .mapToPair(t -> new Tuple2<>(new LongWritable(t._1()), new FloatWritable(t._2())))
                    .saveAsHadoopFile(args[3] + "/" + "cost_" + m[0] + "-" + m[1], LongWritable.class,
                            FloatWritable.class, SequenceFileOutputFormat.class, hadoopConf);
//                while (iter.hasNext()) {
//                    Tuple2<Long, Float> t = iter.next();
//                    costWriter.write(t._1(), t._2());
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
    }

    private static class CostWriter implements Closeable {

        DataOutputStream out;
        long lastId = 0;

        public CostWriter(DataOutputStream out) {
            this.out = out;
        }

        public void write(long id, float data) {
            if (lastId > id) {
                throw new IllegalStateException();
            }
            lastId = id;
            try {
                out.writeFloat(data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class LongComparator implements Comparator<Long>, Serializable {
        @Override
        public int compare(Long o1, Long o2) {
            return Long.compare(o1, o2);
        }
    }

    private static Tuple2<Long, Float> calcCost(Tuple2<Long, int[]> from, Tuple2<Long, int[]> to, KuhnMunkres kmAlg,
                                                double[] dataSizes) {
        long id = from._1().intValue() << 32;
        id = id | (long) from._1().intValue();
        return new Tuple2<>(id, (float) packGain(from._2(), to._2(), kmAlg, dataSizes));
    }

    private static double packGain(int[] p1, int[] p2, KuhnMunkres kmAlg, double[] dataSizes) {
        Range[] pack1 = convertPack(p1), pack2 = convertPack(p2);
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j], dataSizes);
            }
        }
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        return maxWeight[0];
    }

    private static Range[] convertPack(int[] pack) {
        Range[] ret = new Range[pack.length];
        int start = 0;
        for (int i = 0; i < pack.length; i++) {
            int end = start + pack[i];
            ret[i] = new Range(start, end - 1);
            start = end;
        }
        return ret;
    }

    private static double overlap(Range r1, Range r2, double[] dataSizes) {
        if (r1.end < r2.start || r1.start > r2.end) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }

    private static class Range {
        public final int start;
        public final int end;

        Range(int start, int end) {
            if (start > end) {
                throw new IllegalArgumentException("start=" + start + ", end=" + end);
            }
            this.start = start;
            this.end = end;
        }

        boolean contains(int v) {
            return start <= v && v <= end;
        }

        @Override
        public String toString() {
            return "[" + start + "," + end + "]";
        }
    }

    private static int[] parse(String s, String regex) {
        return Stream.of(s.split(regex)).mapToInt(Integer::parseInt).toArray();
    }

}
