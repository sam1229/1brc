package org.sameeran;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import static java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        var map = new HashMap<String,double[]>();

        BiFunction<double[], double[], double[]> fn = (x, y) -> {
            y[0] = Math.min(y[0], x[0]);
            y[1] = Math.max(y[1], x[1]);
            y[2] = x[2] + y[2];
            y[3] = x[3] + y[3];
            return y;
        };

        int nrec = 0;
        try(var reader = new BufferedReader(new FileReader("src/main/resources/measurements_100M.txt"), 8192*4)) {

            for (var line = reader.readLine(); line != null; line = reader.readLine()) {
                var lineArr = line.split(";");
                var k = lineArr[0];
                var v = Float.parseFloat(lineArr[1]);

                map.merge(k, new double[]{v,v,v,1}, fn);
                nrec++;
            }
        } catch (Exception e) {
            System.out.println(Arrays.stream(e.getStackTrace()).sequential());
        }

        var mid = System.currentTimeMillis();
        int finalNrec = nrec;
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
}