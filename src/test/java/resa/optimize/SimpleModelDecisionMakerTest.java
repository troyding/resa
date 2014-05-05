package resa.optimize;

import backtype.storm.StormSubmitter;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import org.junit.Test;
import resa.topology.RandomSentenceSpout;
import resa.util.ResaConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Tom.fu on 5/5/2014.
 */
public class SimpleModelDecisionMakerTest {
    private TopologyBuilder builder = new TopologyBuilder();
    private Map<String, Object> conf = ResaConfig.create(true);
    private Map<Integer, String> t2c = new HashMap<>();

    @Test
    public void testInit() throws Exception {

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
        t2c.put(3, "counter");
        t2c.put(4, "counter");

        t2c.put(5, "split");
        t2c.put(6, "split");
        t2c.put(7, "split");
        t2c.put(8, "split");

        GeneralTopologyContext gtc = new GeneralTopologyContext(builder.createTopology(), conf, t2c, null, null, "ta1wc");
        SimpleModelDecisionMaker smdm = new  SimpleModelDecisionMaker();
        smdm.init(conf, gtc);

    }

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

        Map<String, List<Integer>> c2tasks = t2c.entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue,
                Collectors.mapping(Map.Entry::getKey,Collectors.toList())));
        GeneralTopologyContext gtc = new GeneralTopologyContext(builder.createTopology(), conf, t2c, c2tasks, null, "ta1wc");
        SimpleModelDecisionMaker smdm = new  SimpleModelDecisionMaker();
        smdm.init(conf, gtc);

        String host = "192.168.0.31";
        int port = 6379;
        String queue = "ta1wc";
        int maxLen = 50;

        Map<String, Integer> currAllocation = new HashMap<>();
        currAllocation.put("counter", 2);
        currAllocation.put("split", 4);

        System.out.println( smdm.make(RedisDataSource.readData(host, port, queue, maxLen), 6, currAllocation));

    }
}
