package resa.util;

import backtype.storm.Config;
import backtype.storm.scheduler.Topologies;
import backtype.storm.utils.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TopologyHelperTest {

    private static Config config;

    @BeforeClass
    public static void loadConf() {
        config = new Config();
        config.putAll(Utils.readStormConfig());
        config.put(Config.NIMBUS_HOST, "192.168.0.30");
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
    }

    @Test
    public void testGetTask2Host() {
        System.out.println(TopologyHelper.getTask2Host(config, "fpt-4-1406796003"));
    }

    @Test
    public void testGetTopologyDetails() throws Exception {
        Topologies topologies = TopologyHelper.getTopologyDetails(ResaConfig.create(true));
        topologies.getTopologies().stream().forEach(t -> {
            System.out.println(t.getName() + ":" + t.getNumWorkers());
        });
    }
}