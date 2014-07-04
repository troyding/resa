package resa.topology.video;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import resa.util.ConfigUtil;
import resa.util.Counter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static resa.topology.video.Constant.*;

/**
 * Created by ding on 14-7-3.
 */
public class FrameMatcher extends BaseRichBolt {

    private static class FrameContext {
        String frameId;
        int count = -1;
        int curr = 0;
        Map<Integer, Counter> imageCounter = new HashMap<>();

        FrameContext(String frameId) {
            this.frameId = frameId;
        }

        void update(int[] imgIds) {
            curr++;
            for (int i = 0; i < imgIds.length; i++) {
                imageCounter.computeIfAbsent(i, (k) -> new Counter()).incAndGet();
            }
        }

        boolean isFinish() {
            return count == curr;
        }
    }

    private Map<String, FrameContext> pendingFrames;
    private OutputCollector collector;
    private double minPercentage;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        pendingFrames = new HashMap<>();
        this.collector = collector;
        minPercentage = ConfigUtil.getDouble(stormConf, CONF_MATCH_RATIO, 0.5);
    }

    @Override
    public void execute(Tuple input) {
        FrameContext fCtx = pendingFrames.computeIfAbsent(input.getStringByField(FIELD_FRAME_ID),
                (k) -> new FrameContext(k));
        switch (input.getSourceStreamId()) {
            case STREAM_FEATURE_COUNT:
                fCtx.count = input.getIntegerByField(FIELD_FEATURE_CNT);
                break;
            case STREAM_MATCH_IMAGES:
                fCtx.update((int[]) input.getValueByField(FIELD_MATCH_IMAGES));
                break;
            default:
                throw new IllegalStateException("Bad stream");
        }
        if (fCtx.isFinish()) {
            String out = fCtx.frameId + ":" + fCtx.imageCounter.entrySet().stream()
                    .filter(e -> (double) e.getValue().get() / fCtx.count > minPercentage)
                    .map(e -> e.getKey().toString()).collect(Collectors.joining(","));
            System.out.println(out);
            // just for metrics output
            collector.emit(new Values(out));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("out"));
    }
}
