package resa.optimize;

import backtype.storm.Config;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import org.junit.Test;
import resa.topology.RandomSentenceSpout;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Tom.fu on 5/5/2014.
 */
public class SimpleModelDecisionMakerTest {
    private TopologyBuilder builder = new TopologyBuilder();
    private Map<String, Object> conf = ResaConfig.create(true);
    private Map<Integer, String> t2c = new HashMap<>();

    @Test
    public void testMake() throws Exception {
        int numWorkers = 3;
        int numAckers = 1;

        conf.put(Config.TOPOLOGY_WORKERS, 3);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);

        IRichSpout spout = new RandomSentenceSpout();
        builder.setSpout("sentenceSpout", spout, 1);

        double split_mu = 10.0;
        IRichBolt splitBolt = new TASplitSentence(() -> (long) (-Math.log(Math.random()) * 1000.0 / split_mu));
        builder.setBolt("split", splitBolt, 4).shuffleGrouping("sentenceSpout");

        double counter_mu = 5.0;
        IRichBolt wcBolt = new TAWordCounter(() -> (long) (-Math.log(Math.random()) * 1000.0 / counter_mu));
        builder.setBolt("counter", wcBolt, 2).shuffleGrouping("split");
        t2c.clear();
        t2c.put(5, "sentenceSpout");

        t2c.put(3, "counter");
        t2c.put(4, "counter");

        t2c.put(6, "split");
        t2c.put(7, "split");
        t2c.put(8, "split");
        t2c.put(9, "split");

        Map<String, Integer> currAllocation = new HashMap<>();
        currAllocation.put("counter", 2);
        currAllocation.put("split", 4);
        currAllocation.put("sentenceSpout", 1);

        SimpleModelDecisionMaker smdm = new SimpleModelDecisionMaker();
        smdm.init(conf, currAllocation, builder.createTopology());

        String host = "192.168.0.31";
        int port = 6379;
        String queue = "ta1wc";
        int maxLen = 50;

        Map<String, List<ExecutorDetails>> comp2Executors = new HashMap<>();
        comp2Executors.put("counter", Arrays.asList(new ExecutorDetails(3, 3), new ExecutorDetails(4, 4)));
        comp2Executors.put("sentenceSpout", Arrays.asList(new ExecutorDetails(5, 5)));
        comp2Executors.put("split", Arrays.asList(new ExecutorDetails(6, 6), new ExecutorDetails(7, 7),
                new ExecutorDetails(8, 8), new ExecutorDetails(9, 9)));

        AggResultCalculator resultCalculator = new AggResultCalculator(
                RedisDataSource.readData(host, port, queue, maxLen), comp2Executors, builder.createTopology());
        resultCalculator.calCMVStat();
        System.out.println(smdm.make(resultCalculator.getResults(), 6));

    }

    @Test
    public void testMakeUsingTopologyHelper() throws Exception {


        conf.put(Config.NIMBUS_HOST, "192.168.0.31");
        conf.put(Config.NIMBUS_THRIFT_PORT, 6627);

        GeneralTopologyContext gtc = TopologyHelper.getGeneralTopologyContext("ta1wc", conf);

        if (gtc == null) {
            System.out.println("gtc is null");
            return;
        }

        Map<String, Integer> currAllocation = new HashMap<>();
        currAllocation.put("counter", 2);
        currAllocation.put("split", 4);
        currAllocation.put("sentenceSpout", 1);

        SimpleModelDecisionMaker smdm = new SimpleModelDecisionMaker();
        smdm.init(conf, currAllocation, gtc.getRawTopology());

        String host = "192.168.0.31";
        int port = 6379;
        String queue = "ta1wc";
        int maxLen = 50;

        Map<String, List<ExecutorDetails>> comp2Executors = TopologyHelper.getTopologyExecutors("ta1wc", conf);
        AggResultCalculator resultCalculator = new AggResultCalculator(
                RedisDataSource.readData(host, port, queue, maxLen), comp2Executors, builder.createTopology());
        resultCalculator.calCMVStat();
        System.out.println(smdm.make(resultCalculator.getResults(), 6));

    }
}
