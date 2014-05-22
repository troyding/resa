package resa.metrics;

import backtype.storm.Config;
import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.utils.Utils;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.Sampler;

import java.util.List;
import java.util.Map;

/**
 * A measurable spout implementation based on system hook
 * <p>
 * Created by ding on 14-4-8.
 */
public class MeasurableSpout implements IRichSpout {

    private class MeasurableMsgId {
        final String stream;
        final Object msgId;
        final long startTime;

        private MeasurableMsgId(String stream, Object msgId, long startTime) {
            this.stream = stream;
            this.msgId = msgId;
            this.startTime = startTime;
        }

        public boolean isSampled() {
            return startTime > 0;
        }
    }

    private class SpoutHook extends BaseTaskHook {

        @Override
        public void spoutAck(SpoutAckInfo info) {
            MeasurableMsgId streamMsgId = (MeasurableMsgId) info.messageId;
            if (streamMsgId != null && streamMsgId.isSampled()) {
                completeMetric.addMetric(streamMsgId.stream, System.currentTimeMillis() - streamMsgId.startTime);
            }
        }
    }

    private IRichSpout delegate;
    private transient CMVMetric completeMetric;
    private Sampler sampler;
    private transient MultiCountMetric emitMetric;
    private long lastMetricsSent;

    public MeasurableSpout(IRichSpout delegate) {
        this.delegate = delegate;
    }

    private long getMetricsDuration() {
        long now = System.currentTimeMillis();
        long duration = now - lastMetricsSent;
        lastMetricsSent = now;
        return duration;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        int interval = Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        completeMetric = context.registerMetric(MetricNames.COMPLETE_LATENCY, new CMVMetric(), interval);
        emitMetric = context.registerMetric(MetricNames.EMIT_COUNT, new MultiCountMetric(), interval);
        lastMetricsSent = System.currentTimeMillis();
        context.registerMetric(MetricNames.DURATION, this::getMetricsDuration, interval);
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        context.addTaskHook(new SpoutHook());
        delegate.open(conf, context, new SpoutOutputCollector(collector) {

            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                return super.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                super.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            private MeasurableMsgId newStreamMessageId(String stream, Object messageId) {
                long startTime;
                if (sampler.shoudSample()) {
                    startTime = System.currentTimeMillis();
                    emitMetric.scope(stream).incr();
                } else {
                    startTime = -1;
                }
                return messageId == null ? null : new MeasurableMsgId(stream, messageId, startTime);
            }
        });
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void activate() {
        delegate.activate();
    }

    @Override
    public void deactivate() {
        delegate.deactivate();
    }

    @Override
    public void nextTuple() {
        delegate.nextTuple();
    }

    private Object getUserMsgId(Object msgId) {
        return msgId != null ? ((MeasurableMsgId) msgId).msgId : msgId;
    }

    @Override
    public void ack(Object msgId) {
        delegate.ack(getUserMsgId(msgId));
    }

    @Override
    public void fail(Object msgId) {
        delegate.fail(getUserMsgId(msgId));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }
}
