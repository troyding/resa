package resa.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import resa.metrics.RedisMetricsCollector;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class TAExpServWCLoopRedis {

    public static void main(String[] args) throws Exception {

        Config conf = ConfigUtil.readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }

        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);

        TopologyBuilder builder = new ResaTopologyBuilder();

        int numWorkers = ConfigUtil.getInt(conf, "a4-worker.count", 1);
        int numAckers = ConfigUtil.getInt(conf, "a4-acker.count", 1);

        resaConfig.setNumWorkers(numWorkers);
        resaConfig.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("a4-redis.queue");

        builder.setSpout("sentenceSpout", new TASentenceSpout(host, port, queue),
                ConfigUtil.getInt(conf, "a4-spout.parallelism", 1));

        double split_mu = ConfigUtil.getDouble(conf, "a4-split.mu", 1.0);
        builder.setBolt("split", new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / split_mu)),
                ConfigUtil.getInt(conf, "a4-split.parallelism", 1))
                .setNumTasks(10)
                .shuffleGrouping("sentenceSpout");

        double counter_mu = ConfigUtil.getDouble(conf, "a4-counter.mu", 1.0);
        builder.setBolt("counter",
                new TAWordCounter2Path(() -> (long) (-Math.log(Math.random()) * 1000.0 / counter_mu),
                ConfigUtil.getDouble(conf, "a4-loopback.prob", 0.0)),
                ConfigUtil.getInt(conf, "a4-counter.parallelism", 1))
                .setNumTasks(10)
                .shuffleGrouping("split").shuffleGrouping("counter", "Bolt-P");

        resaConfig.registerMetricsConsumer(RedisMetricsCollector.class, null, 1);
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }
}
