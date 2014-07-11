package resa.scheduler;

import backtype.storm.generated.StormTopology;
import resa.optimize.AllocResult;
import resa.util.ConfigUtil;

import java.util.Map;

/**
 * Created by ding on 14-7-11.
 */
public class OnceAdjustMaker implements DecisionMaker {

    private long startTime;
    private boolean adjust = false;
    private long minAdjustInterval;

    @Override
    public void init(Map<String, Object> conf, StormTopology rawTopology) {
        startTime = now();
        minAdjustInterval = ConfigUtil.getInt(conf, "resa.adjust.min.sec", 300);
    }

    @Override
    public Map<String, Integer> make(AllocResult newAllocResult, Map<String, Integer> currAlloc) {
        if (!adjust && now() - startTime > minAdjustInterval) {
            adjust = true;
            return newAllocResult.currOptAllocation;
        }
        return null;
    }

    private static long now() {
        return System.currentTimeMillis() / 1000;
    }

}
