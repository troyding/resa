package resa.optimize;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by ding on 14-1-27.
 */
public abstract class TASleepBolt extends BaseRichBolt {

    protected transient OutputCollector collector;
    private IntervalSupplier sleep;

    public TASleepBolt(IntervalSupplier sleep) {
        this.sleep = sleep;
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        long inter = this.sleep.get();
        if (inter > 0) {
            Utils.sleep(inter);
        }
    }
}
