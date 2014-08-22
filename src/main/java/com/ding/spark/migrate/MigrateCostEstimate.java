package com.ding.spark.migrate;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by ding on 14-8-20.
 */
public class MigrateCostEstimate {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        conf.set("spark.liveListenerBus.eventQueueCapacity", 1000000 + "");
        conf.set("spark.kryo.registrator", MyKryoRegistrator.class.getName());
        JavaSparkContext context = new JavaSparkContext(conf);
        context.setCheckpointDir("/tmp/spark");
        float gamma = 0.9f;
        double[] dataSizes = Files.readAllLines(Paths.get(args[0])).stream().map(String::trim)
                .filter(s -> !s.isEmpty()).mapToDouble(Double::parseDouble).toArray();
        double totalDataSize = DoubleStream.of(dataSizes).sum();
        TreeMap<String, Double> migrationMetrics = Files.readAllLines(Paths.get(args[1])).stream()
                .map(s -> s.split(":")).collect(Collectors.toMap(strs -> strs[0] + "-" + strs[1],
                        strs -> Double.parseDouble(strs[2]), (v1, v2) -> {
                            throw new IllegalStateException();
                        }, TreeMap::new));
        int[] states = migrationMetrics.keySet().stream().flatMap(s -> Stream.of(s.split("-"))).distinct()
                .mapToInt(Integer::parseInt).sorted().toArray();
        // parse all states
        Map<Integer, JavaPairRDD<Integer, int[]>> statePacksRDD = createStateRDDs(args[2], context, states);
        Map<Integer, Long> stateNumPacks = statePacksRDD.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
                e -> e.getValue().count()));
        System.out.println("Finished statePacksRDD, states=" + stateNumPacks);
        // calc state pack costs first, avoid duplicated computation
        Map<String, JavaPairRDD<Long, Float>> costMap = createCostRDDs(dataSizes, totalDataSize, migrationMetrics,
                stateNumPacks, statePacksRDD);
        System.out.println("Finished costMap, migration=" + costMap.keySet());
        // init state values
        // values data is small, so broadcast it for performance consideration
        Map<Integer, Broadcast<float[]>> stateValues = new HashMap<>();
//        float initValue = (Float) context.union(costMap.values().toArray(new JavaPairRDD[costMap.size()])).values()
//                .min((Comparator & Serializable) ((v1, v2) -> Float.compare((Float) v1, (Float) v2))) * 9;
        float initValue = 0;
        System.out.println("Init value is " + initValue);
        stateNumPacks.forEach((state, numPacks) -> {
            float[] initValues = new float[numPacks.intValue()];
            Arrays.fill(initValues, initValue);
            stateValues.put(state, context.broadcast(initValues));
        });
        int maxNumIteration = 60;
        float threshold = 5.0f;
        System.out.println("Start iteration @ " + new Date());
        for (int i = 0; i < maxNumIteration; i++) {
            float diff = 0.0f;
            for (int state : states) {
                float[] newValues = computeNewValues(context, gamma, getTargetStates(state, migrationMetrics), state,
                        costMap, stateValues);
                Broadcast<float[]> oldBroadcastedValues = stateValues.put(state, context.broadcast(newValues));
                float[] oldValues = oldBroadcastedValues.getValue();
                for (int j = 0; j < newValues.length; j++) {
                    diff += Math.abs(newValues[j] - oldValues[j]);
                }
                oldBroadcastedValues.destroy(false);
            }
            if (diff < threshold) {
                break;
            }
            System.out.println("iteration " + i + ", with diff " + diff);
        }
        System.out.println("iteration end @ " + new Date());
        System.out.println("Writing out values");
        Path outDir = Paths.get(args[3]);
        if (!Files.exists(outDir)) {
            Files.createDirectories(outDir);
        }
        stateValues.forEach((state, vaules) -> writeStateValue(outDir.resolve("values_" + state), vaules.getValue()));
        System.out.println("Done");
    }


    private static void writeStateValue(Path file, float[] values) {
        try (BufferedWriter writer = Files.newBufferedWriter(file)) {
            for (int i = 0; i < values.length; i++) {
                writer.write(String.valueOf(values[i]));
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static float[] computeNewValues(JavaSparkContext context, float gamma, Map<Integer, Double> targetStates,
                                            int currState, Map<String, JavaPairRDD<Long, Float>> costMap,
                                            Map<Integer, Broadcast<float[]>> stateValues) {
        List<JavaPairRDD<Integer, Float>> rdds = new ArrayList<>(targetStates.size());
        targetStates.forEach((targetState, p) -> {
            boolean first = currState < targetState;
            Broadcast<float[]> values = stateValues.get(targetState);
            JavaPairRDD<Long, Float> costRDD = costMap.get(makeCostKey(currState, targetState));
            if (costRDD == null) {
                throw new IllegalStateException("Cannot find cost rdd for " + makeCostKey(currState, targetState));
            }
            JavaPairRDD<Integer, Float> rdd = costRDD
                    .mapToPair(t -> new Tuple2<>(parsePackId(t._1(), first),
                            (t._2() + values.getValue()[parsePackId(t._1(), !first)] * gamma) * p.floatValue()))
                    .reduceByKey((v1, v2) -> Math.min(v1, v2))
                    .persist(StorageLevel.MEMORY_AND_DISK_SER());
            rdds.add(rdd);
        });
        JavaPairRDD<Integer, Float> resultRDD = rdds.remove(0);
        if (!rdds.isEmpty()) {
            resultRDD = context.union(resultRDD, rdds).reduceByKey((v1, v2) -> v1 + v2);
        }
        List<Float> tmpValues = resultRDD.sortByKey().values().collect();
        float[] newValues = new float[tmpValues.size()];
        for (int j = 0; j < newValues.length; j++) {
            newValues[j] = tmpValues.get(j);
        }
        return newValues;
    }

    private static Map<String, JavaPairRDD<Long, Float>> createCostRDDs(double[] dataSizes, double totalSize,
                                                                        SortedMap<String, Double> migrationMetrics,
                                                                        Map<Integer, Long> stateNumPacks,
                                                                        Map<Integer, JavaPairRDD<Integer, int[]>> statePacksRDD) {
        Map<String, JavaPairRDD<Long, Float>> costMap = new HashMap<>();
        // cost from state 2 to state 3 is equals that from state 3 to state 2
        // so we need to do distinct first
        migrationMetrics.keySet().stream().map(s -> s.split("-"))
                .map(s -> new int[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])})
                .map(m -> makeCostKey(m[0], m[1])).distinct().map(s -> s.split("-"))
                .map(s -> new int[]{Integer.parseInt(s[0]), Integer.parseInt(s[1])})
                .forEach(m -> costMap.computeIfAbsent(makeCostKey(m[0], m[1]), k -> {
                    KuhnMunkres km = new KuhnMunkres(dataSizes.length);
                    long numRecords = stateNumPacks.get(m[0]) * stateNumPacks.get(m[1]);
                    // one float and one long in kryo at least costs about 16 bytes
                    int part = (int) (numRecords * 16L / (32L * 1024 * 1024));
                    JavaPairRDD<Long, Float> costRDD = statePacksRDD.get(m[0]).cartesian(statePacksRDD.get(m[1]))
                            .mapToPair(t -> stateCost(t._1(), t._2(), km, dataSizes, totalSize))
                            .coalesce(Math.max(64, part), true).persist(StorageLevel.MEMORY_AND_DISK_SER());
                    costRDD.checkpoint();
                    return costRDD;
                }));
        return costMap;
    }

    private static Map<Integer, JavaPairRDD<Integer, int[]>> createStateRDDs(String rootPath, JavaSparkContext context,
                                                                             int[] states) {
        Map<Integer, JavaPairRDD<Integer, int[]>> statePacksRDD = new HashMap<>();
        org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(rootPath);
        for (int state : states) {
            String file = new org.apache.hadoop.fs.Path(hdfsPath, "state_" + state).toUri().toString();
            JavaPairRDD<Integer, int[]> rdd = context.textFile(file, 32).filter(s -> !s.isEmpty())
                    .map(str -> str.split(":")).mapToPair(strArray -> new Tuple2<>(Integer.valueOf(strArray[0]),
                            parseAndValid(strArray[1], ",", state))).persist(StorageLevel.MEMORY_AND_DISK_SER());
            statePacksRDD.put(state, rdd);
        }
        return statePacksRDD;
    }

    private static int parsePackId(long combinedId, boolean first) {
        return first ? (int) ((combinedId >> 32) & 0xFFFFFFFF) : (int) (combinedId & 0xFFFFFFFF);
    }

    private static Map<Integer, Double> getTargetStates(int curr, SortedMap<String, Double> migrationMetrics) {
        Map<Integer, Double> states = migrationMetrics.subMap(curr + "-", curr + "~").entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.parseInt(e.getKey().split("-")[1]), e -> e.getValue()));
        double sum = states.values().stream().mapToDouble(d -> d).sum();
        for (Map.Entry<Integer, Double> entry : states.entrySet()) {
            double newValue = entry.getValue() / sum;
            entry.setValue(newValue);
        }
        return states;
    }

    private static String makeCostKey(int first, int sec) {
        if (first == sec) {
            throw new IllegalArgumentException("Two state equals: " + first);
        }
        return first < sec ? first + "-" + sec : sec + "-" + first;
    }

    private static Tuple2<Long, Float> stateCost(Tuple2<Integer, int[]> from, Tuple2<Integer, int[]> to,
                                                 KuhnMunkres kmAlg, double[] dataSizes, double totalSize) {
        long id = from._1().longValue() << 32;
        id = id | to._1().longValue();
        return new IdCostPair(id, (float) totalSize - packGain(from._2(), to._2(), kmAlg, dataSizes));
//        return new Tuple2<>(id, (float) totalSize - packGain(from._2(), to._2(), kmAlg, dataSizes));
    }

    private static float packGain(int[] p1, int[] p2, KuhnMunkres kmAlg, double[] dataSizes) {
        Range[] pack1 = convertPack(p1), pack2 = convertPack(p2);
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j], dataSizes);
            }
        }
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        return (float) maxWeight[0];
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

    private static class IdCostPair extends Tuple2<Long, Float> {

        public IdCostPair(Long _1, Float _2) {
            super(_1, _2);
        }

        @Override
        public boolean canEqual(Object that) {
            return that instanceof IdCostPair;
        }

        @Override
        public boolean equals(Object that) {
            IdCostPair obj = (IdCostPair) that;
            return _1().equals(obj._1()) && _2().equals(obj._2());
        }
    }

    private static class IdCostPairSerializer extends Serializer<IdCostPair> {

        @Override
        public void write(Kryo kryo, Output output, IdCostPair object) {
            output.writeLong(object._1());
            output.writeFloat(object._2());
        }

        @Override
        public IdCostPair read(Kryo kryo, Input input, Class<IdCostPair> type) {
            return new IdCostPair(input.readLong(), input.readFloat());
        }
    }

    public static class MyKryoRegistrator implements KryoRegistrator {
        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(IdCostPair.class, new IdCostPairSerializer());
        }
    }

    private static int[] parse(String s, String regex) {
        return Stream.of(s.split(regex)).mapToInt(Integer::parseInt).toArray();
    }

    private static int[] parseAndValid(String s, String regex, int length) {
        int[] ret = parse(s, regex);
        if (ret.length != length) {
            throw new IllegalArgumentException("require length " + length + ", got " + ret.length);
        }
        return ret;
    }

}
