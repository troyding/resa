package resa.topology.join;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import resa.topology.RedisQueueSpout;


/**
 * Created by ding on 14-6-5.
 */
public class RecordSpout extends RedisQueueSpout implements Constant {

    public RecordSpout(String host, int port, String queue) {
        super(host, port, queue);
    }

    @Override
    protected void emitData(Object data) {
        collector.emit(new Values(data), "");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD_RECORD));
    }
}
