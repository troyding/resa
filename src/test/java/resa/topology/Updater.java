package resa.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by ding on 14-3-14.
 */
public class Updater implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

    private OutputCollector collector;
    private Map<String, List<BitSet>> padding;
    private int projectionSize;
    private Map<String, List<BitSet>> fakePadding;

    public Updater(int projectionSize) {
        this.projectionSize = projectionSize;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        padding = new HashMap<>();
        fakePadding = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getValueByField(ObjectSpout.TIME_FILED) + "-" + input.getValueByField(ObjectSpout.ID_FILED);
        List<BitSet> ret = padding.get(key);
        if (ret == null) {
            ret = new ArrayList<>();
            padding.put(key, ret);
        }
        ret.add((BitSet) input.getValueByField(Detector.OUTLIER_FIELD));
        if (ret.size() == projectionSize) {
            padding.remove(key);
            BitSet result = ret.get(0);
            ret.stream().forEach((bitSet) -> {
                if (result != bitSet) {
                    result.or(bitSet);
                }
            });
            // output
            //System.out.println(result);
//            result.stream().forEach((status) -> {
//                if (status == 0) {
//                    // output
//                    collector.emit(new Values());
//                }
//            });
        } else {
            //TODO: Added by Tom Fu to emulate the real calculation time
            // so that measurement results at the initial state will not be biased, only occur at the beginning
            //TODO: Jianbin, please double check!
            List<BitSet> fakeRet = fakePadding.get(key);
            if (fakeRet == null) {
                fakeRet = new ArrayList<>();
                fakePadding.put(key, fakeRet);
            }
            fakeRet.add((BitSet) input.getValueByField(Detector.OUTLIER_FIELD));
            int compensation = projectionSize / (ret.size() + 1) + 1;
            for (int i = 0; i < compensation; i++) {
                BitSet fakeResult = fakeRet.get(0);
                fakeRet.stream().forEach((bitSet) -> {
                    if (fakeResult != bitSet) {
                        fakeResult.or(bitSet);
                    }
                });
            }
            LOG.info("enter Updater->execute->ret.size(): " + ret.size() + ", pSize: " + projectionSize + ", Compensation: " + compensation);
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
