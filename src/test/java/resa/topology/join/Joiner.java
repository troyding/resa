package resa.topology.join;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-8-13.
 */
public class Joiner extends BaseRichBolt implements Constant {

    private static class Region {

    }

    private Map<Integer, Region> regions;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        regions = (Map<Integer, Region>) context.getExecutorData("regions");
        if (regions == null) {
            context.setTaskData("regions", (regions = new HashMap<>()));
        }
    }

    @Override
    public void execute(Tuple input) {
        Region region = regions.computeIfAbsent(input.getIntegerByField(FIELD_REGION), k -> new Region());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
