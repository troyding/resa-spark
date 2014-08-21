package com.ding.spark.migrate;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by ding on 14-8-21.
 */
public class Test {

    public static void main(String[] args) {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(
                Paths.get("/Users/ding/Downloads/values/Value-8.txt")))) {
            while (in.available() > 0) {
                System.out.println(in.readFloat());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        long id = genPackId(1789, 9954);
//        System.out.println(id);
//        System.out.println(parsePackId(id, true));
//        System.out.println(parsePackId(id, false));
    }

    private static int parsePackId(long combinedId, boolean first) {
        return first ? (int) ((combinedId >> 32) & 0xFFFFFFFF) : (int) (combinedId & 0xFFFFFFFF);
    }

    private static long genPackId(int from, int to) {
        long id = Integer.valueOf(from).longValue() << 32;
        id = id | (long) to;
        return id;
    }

}
