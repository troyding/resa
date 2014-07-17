package resa.topology.simulate;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.Map;

/**
 * Created by ding on 14-7-1.
 */
public class SimulatedBolt extends BaseRichBolt {

    public SimulatedBolt(double lambda) {
        this.lambda = lambda;
    }

    private OutputCollector collector;
    private double lambda;
    private RandomDataGenerator generator;
    private long bound;

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;
        generator = new RandomDataGenerator();
        bound = (long) (lambda * 20 * 1_000_000L);
    }

    @Override
    public void execute(Tuple tuple) {
        long cost;
        long exp = Math.min(generator.nextPoisson(lambda) * 1_000_000L + 1, bound);
        long now = System.nanoTime();
        do {
            for (int i = 0; i < 10; i++) {
                Math.atan(Math.sqrt(Math.random() * Integer.MAX_VALUE));
            }
        } while ((cost = (System.nanoTime() - now)) > 0 && cost < exp);
        collector.emit(tuple.getValues());
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text"));
    }

}
