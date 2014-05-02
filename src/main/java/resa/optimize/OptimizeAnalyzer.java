package resa.optimize;

import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * Created by ding on 14-4-30.
 */
public abstract class OptimizeAnalyzer {

    protected TopologyContext topologyContext;
    protected Map<String, Object> conf;

    public void init(Map<String, Object> conf, TopologyContext context) {
        this.conf = conf;
        this.topologyContext = context;
    }

    public abstract OptimizeDecision analyze(Iterable<MeasuredData> dataStream, int maxAvailableExectors,
                                             Map<String, Integer> currAllocation);

}
