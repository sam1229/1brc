package org.sameeran;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import static java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class Main {

    private static class Stats {
        int min;
        int avg;
        int max;
        int count;
        Stats(int min, int avg, int max, int count) {
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.count = count;
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        var map = new HashMap<String, Stats>();

        BiFunction<Stats,Stats,Stats> statsFn = (x,y) -> {
            x.min = Math.min(x.min, y.min);
            x.max = Math.max(x.max, y.max);
            x.avg = x.avg + y.avg;
            x.count = x.count + y.count;
            return x;
        };

        int nrec = 0;
        int batchSize = 1000;
        var pool = Executors.newWorkStealingPool();
        try(var reader = new BufferedReader(new FileReader("src/main/resources/measurements_100M.txt"))) {

            for (var line = reader.readLine(); line != null; line = reader.readLine()) {
                // reading a few lines in a batch and parallelizing that batch
                // increased perfomance significantly from ~15s to ~8s
                var stringArr = new String[batchSize];
                stringArr[0] = line;
                for(int i = 1; i < batchSize; i++) {
                    String s = reader.readLine();
                    if(s == null) { break; }
                    stringArr[i] = s;
                }
                pool.submit(() -> update_stat_map(stringArr, map, statsFn));
            }
        } catch (Exception e) {
            System.out.println(Arrays.stream(e.getStackTrace()).sequential());
        }

        pool.close();
        var mid = System.currentTimeMillis();

        var ans = map.entrySet().stream()
            .map(e -> new SimpleImmutableEntry<>(e.getKey(),
                    BigDecimal.valueOf((float)e.getValue().min/10.0).setScale(1, RoundingMode.HALF_UP).toPlainString()
                            .concat("/")
                            .concat(BigDecimal.valueOf((float)e.getValue().max/10).setScale(1, RoundingMode.HALF_UP).toPlainString())
                            .concat("/")
                            .concat(BigDecimal.valueOf((float)e.getValue().avg/(10.0*e.getValue().count))
                                    .setScale(1, RoundingMode.HALF_UP).toPlainString())))
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());

        System.out.println(System.currentTimeMillis() - mid);
        System.out.println(mid - start);
        System.out.println(nrec);
        System.out.println(ans);
    }

    private static void update_stat_map(String[] line, Map<String, Stats> map, BiFunction<Stats, Stats, Stats> fn) {
        for (int i = 0; i < line.length; i++) {
            if(line[i] == null) break;
            var lineArr = splitString(line[i]);
            var k = lineArr[0];
            var v = parseTemp(lineArr[1]);

            map.merge(k, new Stats(v,v,v,1), fn);
        }
    }

    // this (using int instead of float) adds a small performance gain over using floats
    private static int parseTemp(String num) {
        num = num.replace(".","");
        // can't see a significant difference for writing a custom fuction to replace
        // might explore later
        return Integer.parseInt(num);
    }

    private static String[] splitString(String str) {
        var ans = new String[2];
        int i = str.indexOf(';') + 1;
        ans[0] = str.substring(0,i);
        ans[1] = str.substring(i);
        return ans;
    }
}