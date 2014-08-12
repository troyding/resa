package resa.scheduler.plan;

import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static resa.scheduler.plan.PackCalculator.*;
import static resa.scheduler.plan.PackCalculator.convertPack;

/**
 * Created by ding on 14-7-21.
 */
public class MigrateCostEstimate {

    private double[] dataSizes;
    private double[] workload;
    private double totalDataSize;
    private TreeMap<String, Double> migrationMetrics;
    private float ratio = 1.8f;

    @Before
    public void init() throws Exception {
        workload = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/workload-064.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        dataSizes = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/data-sizes-064.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf).toArray();
        totalDataSize = DoubleStream.of(dataSizes).sum();
        migrationMetrics = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/metrics.txt")).stream()
                .map(s -> s.split(":")).collect(Collectors.toMap(strs -> strs[0] + "-" + strs[1],
                        strs -> Double.parseDouble(strs[2]), (v1, v2) -> {
                            throw new IllegalStateException();
                        }, TreeMap::new));
    }

    private int getNextState(int curr) {
        Map.Entry<String, Double>[] states = migrationMetrics.subMap(curr + "-", curr + "~").entrySet()
                .toArray(new Map.Entry[0]);
        double sum = Stream.of(states).mapToDouble(e -> e.getValue()).sum();
        double rand = Math.random();
        double d = 0;
        for (int i = 0; i < states.length; i++) {
            d += (states[i].getValue() / sum);
            if (d >= rand) {
                return Integer.parseInt(states[i].getKey().split("-")[1]);
            }
        }
        throw new IllegalStateException();
    }

    private Map<Integer, Double> getTargetState(int curr) {
        Map<Integer, Double> states = migrationMetrics.subMap(curr + "-", curr + "~").entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.parseInt(e.getKey().split("-")[1]), e -> e.getValue()));
        double sum = states.values().stream().mapToDouble(d -> d).sum();
        for (Map.Entry<Integer, Double> entry : states.entrySet()) {
            double newValue = entry.getValue() / sum;
            entry.setValue(newValue);
        }
        return states;
    }

    private Map<Integer, Double> getNeighState(int curr) {
        Map<Integer, Double> states = new HashMap<>();
        String currState = String.valueOf(curr);
        migrationMetrics.entrySet().stream().filter(e -> e.getKey().contains(currState)).forEach(e -> {
            String[] stateStrs = e.getKey().split("-");
            String nei = stateStrs[0].equals(currState) ? stateStrs[1] : stateStrs[0];
            states.compute(Integer.valueOf(nei), (k, v1) -> v1 == null ? e.getValue() : v1 + e.getValue());
        });
        double sum = states.values().stream().mapToDouble(d -> d).sum();
        for (Map.Entry<Integer, Double> entry : states.entrySet()) {
            double newValue = entry.getValue() / sum;
            entry.setValue(newValue);
        }
        return states;
    }


    @Test
    public void compare() {
        int count = 100;
        int[] states = new int[count];
        states[0] = 8;
        for (int i = 1; i < states.length; i++) {
            states[i] = getNextState(states[i - 1]);
        }
        System.out.println(Arrays.toString(states));
        for (int i = 1; i < states.length; i++) {
            System.out.print(getTargetState(states[i - 1]).get(states[i]));
            System.out.print(",");
        }
        System.out.println();
        KuhnMunkres km = new KuhnMunkres(dataSizes.length);
        System.out.println("local opt: " + output(calcLocalOptimization(states, km), count));
        System.out.println("global opt1: " + output(calcGlobalOptimization(states, km), count));
    }

    private String output(double toMove, int count) {
        toMove = toMove / count;
        return String.format("Avg to move %dbytes, total %dbytes, rate %f", (long) toMove, (long) totalDataSize,
                toMove / totalDataSize);
    }


    private double calcGlobalOptimization(int[] states, KuhnMunkres km) {
        Map<Integer, int[]> state2Pack = IntStream.of(null).boxed()
                .collect(Collectors.toMap(i -> i, i -> packAvg(workload.length, i)));
        double toMove = 0.0;
        for (int i = 1; i < states.length; i++) {
            Range[] currPack = convertPack(state2Pack.get(states[i - 1]));
            calcBest1(states[i - 1], states[i], state2Pack);
            double remain = packGain(currPack, convertPack(state2Pack.get(states[i])), km);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            toMove += (totalDataSize - remain);
        }
        return toMove;
    }

    private void calcBest1(int currState, int nextStat, Map<Integer, int[]> state2Pack) {
        int[] states = null;
        Map<Integer, Double> gain = new TreeMap<>();
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        //  Map<Integer, Double> newGain = new HashMap<>();
        int[] initState = Arrays.copyOf(state2Pack.get(currState), currState);
        int j;
        do {
            j = 0;
            for (int i = 0; i < states.length; i++) {
                Map<int[], Double> packs = new HashMap<>();
                getTargetState(states[i]).forEach((state, p) -> packs.put(state2Pack.get(state), p));
                if (states[i] == nextStat) {
//                    for (Map.Entry<int[], Double> entry : packs.entrySet()) {
//                        entry.setValue(entry.getValue() * 0.5);
//                    }
                    packs.put(initState, 1.01);
                }
                calculator.setSrcPacks(packs).setTargetPackSize(states[i]).calc();
                state2Pack.put(states[i], calculator.getPack());
                double g = calculator.gain();
                Double oldGain = gain.put(states[i], g);
                if (oldGain == null || Math.abs(g - oldGain) > 100) {
                    j++;
                }
            }
//            System.out.println(gain);
        } while (j > 0);
//        System.out.println("-----------");
    }

    private double calcLocalOptimization(int[] states, KuhnMunkres km) {
        PackCalculator calculator = new DPBasedCalculator().setWorkloads(workload).setDataSizes(dataSizes)
                .setUpperLimitRatio(ratio);
        double toMove = 0.0;
        int[] srcPack = packAvg(dataSizes.length, states[0]);
        for (int i = 1; i < states.length; i++) {
            calculator.setSrcPack(srcPack).setTargetPackSize(states[i]).calc();
            int[] newPack = calculator.getPack();
            double remain = packGain(convertPack(srcPack), convertPack(newPack), km);
            toMove += (totalDataSize - remain);
//            System.out.printf("%.2f\n", (totalDataSize - remain) / 1024);
            srcPack = newPack;
        }
        return toMove;
    }

    private double packGain(Range[] pack1, Range[] pack2, KuhnMunkres kmAlg) {
        double[][] weights = new double[pack1.length][pack2.length];
        for (int i = 0; i < pack1.length; i++) {
            for (int j = 0; j < pack2.length; j++) {
                weights[i][j] = overlap(pack1[i], pack2[j]);
            }
        }
        double[] maxWeight = new double[1];
        kmAlg.getMaxBipartie(weights, maxWeight);
        return maxWeight[0];
    }

    private double overlap(Range r1, Range r2) {
        if (r1.end < r2.start || r1.start > r2.end) {
            return 0;
        } else if (r1.start <= r2.start && r1.end >= r2.start) {
            return IntStream.rangeClosed(r2.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        } else if (r1.start >= r2.start && r1.start <= r2.end) {
            return IntStream.rangeClosed(r1.start, Math.min(r2.end, r1.end)).mapToDouble(i -> dataSizes[i]).sum();
        }
        return 0;
    }


    private static int[] packAvg(int eleCount, int packCount) {
        int[] ret = new int[packCount];
        Arrays.fill(ret, eleCount / packCount);
        int k = eleCount % packCount;
        for (int i = 0; i < k; i++) {
            ret[i]++;
        }
        return ret;
    }

}
