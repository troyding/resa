package resa.topology.join;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by ding on 14-8-13.
 */
public class RecordParser extends BaseRichBolt implements Constant {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String record = input.getStringByField(FIELD_RECORD);
        String[] tmp = record.split(",");
        if (tmp.length < 7) {
            return;
        }
        float longitude = Float.parseFloat(tmp[2]);
        float latitude = Float.parseFloat(tmp[3]);
        collector.emit(new Values(tmp[0], longitude, latitude, null));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD_TAXI_ID, FIELD_LONGITUDE, FIELD_LATITUDE, FIELD_REGION));
    }
}
