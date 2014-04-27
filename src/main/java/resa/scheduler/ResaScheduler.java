package resa.scheduler;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.DefaultScheduler;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;

import java.util.Map;

/**
 * Created by ding on 14-4-26.
 */
public class ResaScheduler implements IScheduler {

    private TopologyListener topoListener;
    private IScheduler defaultScheduler = new DefaultScheduler();

    @Override
    public void prepare(Map conf) {
        defaultScheduler.prepare(conf);
        topoListener = new TopologyListener(conf);
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        topoListener.synTopologies(topologies);
        defaultScheduler.schedule(topologies, cluster);
    }

}
