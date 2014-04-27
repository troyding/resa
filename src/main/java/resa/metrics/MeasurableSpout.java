package resa.metrics;

import backtype.storm.Config;
import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.spout.ISpoutOutputCollector;
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
 * <p/>
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
    }

    private class SpoutHook extends BaseTaskHook {

        @Override
        public void spoutAck(SpoutAckInfo info) {
            MeasurableMsgId streamMsgId = (MeasurableMsgId) info.messageId;
            if (streamMsgId != null && streamMsgId.startTime > 0) {
                metric.addMetric(streamMsgId.stream, System.currentTimeMillis() - streamMsgId.startTime);
            }
        }
    }

    private IRichSpout delegate;
    private CMVMetric metric;
    private Sampler sampler;

    public MeasurableSpout(IRichSpout delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        metric = context.registerMetric(MetricNames.COMPLETE_LATENCY, new CMVMetric(),
                Utils.getInt(conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS)));
        sampler = new Sampler(ConfigUtil.getDouble(conf, ResaConfig.COMP_SAMPLE_RATE, 0.05));
        context.addTaskHook(new SpoutHook());
        delegate.open(conf, context, new SpoutOutputCollector(new ISpoutOutputCollector() {

            @Override
            public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
                return collector.emit(streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
                collector.emitDirect(taskId, streamId, tuple, newStreamMessageId(streamId, messageId));
            }

            @Override
            public void reportError(Throwable error) {
                collector.reportError(error);
            }

            private MeasurableMsgId newStreamMessageId(String stream, Object messageId) {
                long startTime = sampler.shoudSample() ? System.currentTimeMillis() : -1;
                return messageId == null ? null : new MeasurableMsgId(stream, messageId, startTime);
            }
        }));
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
