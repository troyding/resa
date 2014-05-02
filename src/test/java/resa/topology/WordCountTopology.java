package resa.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

    public static class SplitSentence extends BaseBasicBolt {

        private static final long serialVersionUID = 9182719848878455933L;

        public SplitSentence() {
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String sentence = input.getString(0);
            StringTokenizer tokenizer = new StringTokenizer(sentence, "\\s+");
            while (tokenizer.hasMoreTokens()) {
                collector.emit(Arrays.asList((Object) tokenizer.nextToken()));
            }
            // Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        @Override
        public void cleanup() {
            System.out.println("Split cleanup");
        }
    }

    public static class WordCount extends BaseBasicBolt {
        private static final long serialVersionUID = 4905347466083499207L;
        private Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
            // Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void cleanup() {
            System.out.println("Word Counter cleanup");
        }
    }

    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new ResaTopologyBuilder();
        builder.setSpout("say", new RandomSentenceSpout(), 2);

        builder.setBolt("split", new SplitSentence(), 4).shuffleGrouping("say").setNumTasks(8);
        builder.setBolt("counter", new WordCount(), 3).fieldsGrouping("split", new Fields("word")).setNumTasks(6);

        resaConfig.addOptimizeSupport();
        resaConfig.put(ResaConfig.ANALYZER_CLASS, FakeDecisionMaker.class.getName());
        resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }
}
