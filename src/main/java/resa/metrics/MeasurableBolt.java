package resa.metrics;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.Map;

/**
 * Created by ding on 14-1-27.
 */
public class MeasurableBolt implements IRichBolt {

    private transient CMVMetric executeMetric;
    private IRichBolt delegate;
    private Sampler sampler;

    public MeasurableBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        executeMetric = context.registerMetric(MetricNames.TASK_EXECUTE, new CMVMetric(),
                Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)));
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        delegate.prepare(conf, context, outputCollector);
    }

    @Override
    public void execute(Tuple tuple) {
        long elapse;
        if (sampler.shoudSample()) {
            long arrivalTime = System.nanoTime();
            delegate.execute(tuple);
            elapse = System.nanoTime() - arrivalTime;
        } else {
            elapse = -1;
            delegate.execute(tuple);
        }
        // avoid numerical overflow
        if (elapse > 0) {
            String id = tuple.getSourceComponent() + ":" + tuple.getSourceStreamId();
            executeMetric.addMetric(id, elapse / 1000000.0);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }
}
