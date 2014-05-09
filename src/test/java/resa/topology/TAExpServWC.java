package resa.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
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

        conf.setNumWorkers(numWorkers);
        conf.setNumAckers(numAckers);

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("a1-redis.queue");

        builder.setSpout("sentenceSpout", new TASentenceSpout(host, port, queue),
                ConfigUtil.getInt(conf, "a1-spout.parallelism", 1));

        double split_mu = ConfigUtil.getDouble(conf, "a1-split.mu", 1.0);
        builder.setBolt("split", new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / split_mu)),
                ConfigUtil.getInt(conf, "a1-split.parallelism", 1)).setNumTasks(10).shuffleGrouping("sentenceSpout");

        double counter_mu = ConfigUtil.getDouble(conf, "a1-counter.mu", 1.0);
        builder.setBolt("counter", new TAWordCounter(() -> (long) (-Math.log(Math.random()) * 1000.0 / counter_mu)),
                ConfigUtil.getInt(conf, "a1-counter.parallelism", 1)).setNumTasks(10).shuffleGrouping("split");

//        Map<String, Object> consumerArgs = new HashMap<>();
//        consumerArgs.put(RedisMetricsCollector.REDIS_HOST, host);
//        consumerArgs.put(RedisMetricsCollector.REDIS_PORT, port);
//        consumerArgs.put(ConsumerBase.METRICS_NAME, Arrays.asList("tuple-completed", "__sendqueue", "__receive"));
//        String queueName = (String) conf.get("a1-metrics.output.queue-name");
//        if (queueName != null) {
//            consumerArgs.put(RedisMetricsCollector.REDIS_QUEUE_NAME, queueName);
//        }
//        conf.registerMetricsConsumer(WinAggMeasurementCollector.class, consumerArgs, 1);

        resaConfig.addOptimizeSupport();
        resaConfig.put(ResaConfig.REBALANCE_WAITING_SECS, 0);
        StormSubmitter.submitTopology(args[0], resaConfig, builder.createTopology());
    }
}