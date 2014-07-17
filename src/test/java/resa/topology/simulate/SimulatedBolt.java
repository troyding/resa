package resa.topology.simulate;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by ding on 14-7-1.
 */
public class SimulatedBolt extends TASleepBolt {

    public SimulatedBolt(long computeTime) {
        this.computeTime = computeTime;
    }

    private OutputCollector collector;
    private long computeTime;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        super.prepare(map, context, outputCollector);
        this.collector = outputCollector;
        setIntervalSupplier(() -> computeTime);
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);
        collector.emit(tuple.getValues());
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }

}
