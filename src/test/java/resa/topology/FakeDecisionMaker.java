package resa.topology;

import resa.optimize.AggResult;
import resa.optimize.DecisionMaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-5-1.
 */
public class FakeDecisionMaker extends DecisionMaker {

    @Override
    public Map<String, Integer> make(Map<String, AggResult[]> executorAggResults, int maxAvailableExectors) {
        Map<String, Integer> ret = new HashMap<>(currAllocation);
        ArrayList<Map.Entry<String, Integer>> tmp = new ArrayList<>(ret.entrySet());
        Collections.shuffle(tmp);
        Map.Entry<String, Integer> entry = tmp.get(0);
        int old = entry.getValue();
        int newThreads = System.currentTimeMillis() % 2 == 0 ? old + 1 : old - 1;
        entry.setValue(newThreads);
        System.out.println(entry.getKey() + ": Old is " + old + ", new is " + newThreads);
        return ret;
    }
}
