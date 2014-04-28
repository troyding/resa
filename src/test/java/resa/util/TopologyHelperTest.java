package resa.util;

import backtype.storm.scheduler.Topologies;
import org.junit.Test;

public class TopologyHelperTest {

    @Test
    public void testGetTopologyDetails() throws Exception {
        Topologies topologies = TopologyHelper.getTopologyDetails(ResaConfig.create(true));
        topologies.getTopologies().stream().forEach(t -> {
            System.out.println(t.getName() + ":" + t.getNumWorkers());
        });
    }
}