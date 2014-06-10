package resa.topology.fp;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.resa.util.ConfigUtil;

import java.io.File;

/**
 * Created by ding on 14-6-6.
 */
public class FrequentPatternTopology implements Constant {

    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        LocalCluster cluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ((Number) conf.get("redis.port")).intValue();
        String queue = (String) conf.get("redis.queue");
        builder.setSpout("input", new SentenceSpout(host, port, queue), ConfigUtil.getInt(conf, "fp.spout.parallelism", 1));

        builder.setBolt("generator", new PatternGenerator(), ConfigUtil.getInt(conf, "fp.generator.parallelism", 1))
                .shuffleGrouping("input")
                .setNumTasks(ConfigUtil.getInt(conf, "fp.generator.tasks", 1));
        builder.setBolt("detector", new Detector(), ConfigUtil.getInt(conf, "fp.detector.parallelism", 1))
                .fieldsGrouping("generator", new Fields(PATTERN_FIELD))
                .fieldsGrouping("detector", FEEDBACK_STREAM, new Fields(PATTERN_FIELD))
                .setNumTasks(ConfigUtil.getInt(conf, "fp.detector.tasks", 1));

        builder.setBolt("reporter", new PatternReporter(), ConfigUtil.getInt(conf, "fp.reporter.parallelism", 1))
                .fieldsGrouping("detector", REPORT_STREAM, new Fields(PATTERN_FIELD))
                .setNumTasks(ConfigUtil.getInt(conf, "fp.reporter.tasks", 1));

        ///StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        ///conf.setDebug(true);
        cluster.submitTopology("fp", conf, builder.createTopology());
    }

}
