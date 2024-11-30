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

    private static class Stats {
        float min;
        double avg;
        float max;
        int count;
        Stats(float min, double avg, float max, int count) {
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.count = count;
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        var map = new ConcurrentHashMap<String, Stats>();

        BiFunction<Stats,Stats,Stats> statsFn = (x,y) -> {
            x.min = Math.min(x.min, y.min);
            x.max = Math.max(x.max, y.max);
            x.avg = x.avg + y.avg;
            x.count = x.count + y.count;
            return x;
        };

        int nrec = 0;
        var pool = Executors.newWorkStealingPool();
        try(var reader = new BufferedReader(new FileReader("src/main/resources/measurements_100M.txt"))) {

            for (var line = reader.readLine(); line != null; line = reader.readLine()) {
                String finalLine = line;
                pool.submit(() -> update_stat_map(finalLine, map, statsFn));
            }
        } catch (Exception e) {
            System.out.println(Arrays.stream(e.getStackTrace()).sequential());
        }

        pool.close();

        var mid = System.currentTimeMillis();

        var ans = map.entrySet().stream()
            .map(e -> new SimpleImmutableEntry<>(e.getKey(),
                    BigDecimal.valueOf(e.getValue().min).setScale(1, RoundingMode.HALF_UP).toPlainString()
                            .concat("/")
                            .concat(BigDecimal.valueOf(e.getValue().max).setScale(1, RoundingMode.HALF_UP).toPlainString())
                            .concat("/")
                            .concat(BigDecimal.valueOf(e.getValue().avg/e.getValue().count)
                                    .setScale(1, RoundingMode.HALF_UP).toPlainString())))
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());

        System.out.println(System.currentTimeMillis() - mid);
        System.out.println(mid - start);
        System.out.println(nrec);
        System.out.println(ans);
    }

    private static void update_stat_map(String line,
                                        ConcurrentHashMap<String, Stats> map, BiFunction<Stats, Stats, Stats> fn) {
        var lineArr = splitString(line);
        var k = lineArr[0];
        var v = Float.parseFloat(lineArr[1]);

        map.merge(k, new Stats(v,v,v,1), fn);
    }

    private static String[] splitString(String str) {
        var ans = new String[2];
        int i = str.indexOf(';') + 1;
        ans[0] = str.substring(0,i);
        ans[1] = str.substring(i);
        return ans;
    }
}