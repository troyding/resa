package resa.topology;

import resa.optimize.OptimizeAnalyzer;
import resa.optimize.MeasuredData;
import resa.optimize.OptimizeDecision;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-5-1.
 */
public class FakeOptimizeAnalyzer extends OptimizeAnalyzer {

    @Override
    public OptimizeDecision analyze(Iterable<MeasuredData> dataStream, int maxAvailableExectors,
                                    Map<String, Integer> currAllocation) {
        Map<String, Integer> ret = new HashMap<>(currAllocation);
        Map.Entry<String, Integer> entry = ret.entrySet().stream().findAny().get();
        int old = entry.getValue();
        int newThreads = System.currentTimeMillis() % 2 == 0 ? old + 1 : old - 1;
        entry.setValue(newThreads);
        System.out.println("Old is " + old + ", new is " + newThreads);
        return new OptimizeDecision(OptimizeDecision.Status.FEASIBALE, ret, ret);
    }
}
