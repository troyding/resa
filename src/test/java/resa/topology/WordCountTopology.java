package resa.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import resa.metrics.RedisMetricsCollector;
import resa.migrate.Persistable;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
            String sentence = input.getStringByField("sentence");
            StringTokenizer tokenizer = new StringTokenizer(sentence.replaceAll("\\p{P}|\\p{S}", " "));
            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken().trim();
                if (!word.isEmpty()) {
                    collector.emit(Arrays.asList((Object) word.toLowerCase()));
                }
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

    public static class WordCount extends BaseBasicBolt implements Persistable {
        private static final long serialVersionUID = 4905347466083499207L;
        private int numBuckets = 6;
        private RotatingMap<String, Integer> counters;

        @Override
        public void prepare(Map stormConf, TopologyContext context) {
            super.prepare(stormConf, context);
            counters = new RotatingMap<>(numBuckets);
            int interval = Utils.getInt(stormConf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
            context.registerMetric("number-words", this::getNumWords, interval);
        }

        private long getNumWords() {
            counters.rotate();
            return counters.size();
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getStringByField("word");
            Integer count = counters.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counters.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void cleanup() {
            System.out.println("Word Counter cleanup");
        }

        @Override
        public void save(Kryo kryo, Output output) throws IOException {
            output.writeInt(numBuckets);
            for (int i = 0; i < numBuckets; i++) {
                kryo.writeClassAndObject(output, counters.rotate());
            }
        }

        @Override
        public void read(Kryo kryo, Input input) throws IOException {
        }
    }

    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));
        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new WritableTopologyBuilder();

        if (!ConfigUtil.getBoolean(conf, "spout.redis", false)) {
            builder.setSpout("say", new RandomSentenceSpout(), ConfigUtil.getInt(conf, "spout.parallelism", 1));
        } else {
            String host = (String) conf.get("redis.host");
            int port = ((Number) conf.get("redis.port")).intValue();
            String queue = (String) conf.get("redis.queue");
            builder.setSpout("say", new TASentenceSpout(host, port, queue),
                    ConfigUtil.getInt(conf, "spout.parallelism", 1));
        }
        builder.setBolt("split", new SplitSentence(), ConfigUtil.getInt(conf, "split.parallelism", 1))
                .shuffleGrouping("say");
        builder.setBolt("counter", new WordCount(), ConfigUtil.getInt(conf, "counter.parallelism", 1))
                .fieldsGrouping("split", new Fields("word")).setNumTasks(32);

        resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }
}
