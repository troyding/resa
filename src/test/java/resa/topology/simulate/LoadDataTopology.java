package resa.topology.simulate;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import resa.topology.ResaTopologyBuilder;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static resa.util.ConfigUtil.readConfig;

/**
 * Created by ding on 14-7-29.
 */
public class LoadDataTopology {

    private static StormTopology createTopology(Map<String, Object> conf) {
        TopologyBuilder builder = new ResaTopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = ConfigUtil.getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");
        builder.setSpout("input", new SimulatedSpout(host, port, queue), ConfigUtil.getInt(conf,
                "simulate.spout.parallelism", 1));

        int parallelism = ConfigUtil.getInt(conf, "simulate.bolt.data-loader.parallelism", 1);
        int numTasks = ConfigUtil.getInt(conf, "simulate.bolt.data-loader.tasks", parallelism);
        int numDataChunks = ConfigUtil.getIntThrow(conf, "simulate.bolt.data-loader.data-chunks");
        int toMoveChunks = (int) (ConfigUtil.getDouble(conf, "simulate.bolt.data-loader.ratio", 1.0) * numDataChunks);
        int task = 0;
        Map<String, Integer> task2Data = new HashMap<>();
        for (int i = 0; i < toMoveChunks; i++) {
            task2Data.compute(String.valueOf(task % numTasks), (k, v) -> v == null ? 1 : v + 1);
            task++;
        }
        conf.put("taskToData", task2Data);
        builder.setBolt("data-loader", new DataLoader(), parallelism).shuffleGrouping("input")
                .setNumTasks(numTasks);
        //conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, totalComputeTime * 3);
        return builder.createTopology();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = readConfig(new File(args[1]));

        if (conf == null) {
            throw new RuntimeException("cannot find conf file " + args[1]);
        }
        ResaConfig resaConfig = ResaConfig.create();
        resaConfig.putAll(conf);
        StormTopology topology = createTopology(resaConfig);
        if (args[0].equals("[local]")) {
            resaConfig.setDebug(false);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("local", resaConfig, topology);
        } else {
//            resaConfig.addOptimizeSupport();
//            resaConfig.registerMetricsConsumer(RedisMetricsCollector.class);
            StormSubmitter.submitTopology(args[0], resaConfig, topology);
        }
    }

}
