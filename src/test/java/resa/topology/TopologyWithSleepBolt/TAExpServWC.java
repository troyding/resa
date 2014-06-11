package resa.topology.TopologyWithSleepBolt;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import resa.metrics.RedisMetricsCollector;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class TAExpServWC {


    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new ResaTopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a1-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a1-acker.count", 1);

        resaConfig.setNumWorkers(numWorkers);
        resaConfig.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("a1-redis.queue");

        int defaultTaskNum = ConfigUtil.getInt(conf, "a1-task.default", 10);

        builder.setSpout("sentenceSpout", new TASentenceSpout(host, port, queue),
                ConfigUtil.getInt(conf, "a1-spout.parallelism", 1));

        double split_mu = ConfigUtil.getDouble(conf, "a1-split.mu", 1.0);
        builder.setBolt("split", new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / split_mu)),
                ConfigUtil.getInt(conf, "a1-split.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("sentenceSpout");

        double counter_mu = ConfigUtil.getDouble(conf, "a1-counter.mu", 1.0);
        builder.setBolt("counter", new TAWordCounter(() -> (long) (-Math.log(Math.random()) * 1000.0 / counter_mu)),
                ConfigUtil.getInt(conf, "a1-counter.parallelism", 1))
                .setNumTasks(defaultTaskNum)
                .shuffleGrouping("split");

        if (ConfigUtil.getBoolean(conf, "a1-metric.resa", false)) {
            resaConfig.addOptimizeSupport();
            resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
            System.out.println("ResaMetricsCollector is registered");
        }

        if (ConfigUtil.getBoolean(conf, "a1-metric.redis", true)) {
            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            System.out.println("RedisMetricsCollector is registered");
        }

        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }
}
