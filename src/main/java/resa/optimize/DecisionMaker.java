package resa.optimize;

import backtype.storm.task.GeneralTopologyContext;

import java.util.Map;

/**
 * Created by ding on 14-4-30.
 */
public abstract class DecisionMaker {

    protected GeneralTopologyContext topologyContext;
    protected Map<String, Object> conf;

    public void init(Map<String, Object> conf, GeneralTopologyContext context) {
        this.conf = conf;
        this.topologyContext = context;
    }

    public abstract Map<String, Integer> make(Iterable<MeasuredData> dataStream, int maxAvailableExectors,
                                              Map<String, Integer> currAllocation);

}
