package org.sameeran;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import static java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        var map = new ConcurrentHashMap<String, double[]>();

        BiFunction<double[], double[], double[]> fn = (y, x) -> {
            y[0] = Math.min(y[0], x[0]);
            y[1] = Math.max(y[1], x[1]);
            y[2] = x[2] + y[2];
            y[3] = x[3] + y[3];
            x = null;
            return y;
        };

        int nrec = 0;
        var pool = Executors.newWorkStealingPool();
        try(var reader = new BufferedReader(new FileReader("src/main/resources/measurements_100M.txt"))) {

            for (var line = reader.readLine(); line != null; line = reader.readLine()) {
                String finalLine = line;
                pool.submit(() -> update_stat_map(finalLine, map, fn));
            }
        } catch (Exception e) {
            System.out.println(Arrays.stream(e.getStackTrace()).sequential());
        }

        pool.close();

        var mid = System.currentTimeMillis();

        var ans = map.entrySet().stream()
            .map(e -> new SimpleImmutableEntry<>(e.getKey(),
                    BigDecimal.valueOf(e.getValue()[0]).setScale(1, RoundingMode.HALF_UP).toPlainString()
                            .concat("/")
                            .concat(BigDecimal.valueOf(e.getValue()[1]).setScale(1, RoundingMode.HALF_UP).toPlainString())
                            .concat("/")
                            .concat(BigDecimal.valueOf(e.getValue()[2]/e.getValue()[3])
                                    .setScale(1, RoundingMode.HALF_UP).toPlainString())))
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());

        System.out.println(System.currentTimeMillis() - mid);
        System.out.println(mid - start);
        System.out.println(nrec);
        System.out.println(ans);
    }

    private static void update_stat_map(String line,
                                        ConcurrentHashMap<String, double[]> map, BiFunction<double[], double[], double[]> fn) {
        var lineArr = splitString(line);
        var k = lineArr[0];
        var v = Float.parseFloat(lineArr[1]);

        map.merge(k, new double[]{v,v,v,1}, fn);
    }

    private static String[] splitString(String str) {
        var ans = new String[2];
        int i = str.indexOf(';') + 1;
        ans[0] = str.substring(0,i);
        ans[1] = str.substring(i);
        return ans;
    }
}