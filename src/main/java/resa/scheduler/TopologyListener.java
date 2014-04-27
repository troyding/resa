package resa.scheduler;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;
import resa.util.TopologyHelper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-4-26.
 */
public class TopologyListener {

    private static final Logger LOG = Logger.getLogger(TopologyListener.class);

    public static class AssignmentContext {
        private long lastRequest;
        private long lastRebalance;
        private Map<String, Integer> compExecutors;
        private TopologyDetails topologyDetails;

        public AssignmentContext setCompExecutors(Map<String, Integer> compExecutors) {
            this.compExecutors = compExecutors;
            return this;
        }

        public AssignmentContext setTopologyDetails(TopologyDetails topologyDetails) {
            this.topologyDetails = topologyDetails;
            return this;
        }

        public AssignmentContext(TopologyDetails topologyDetails, Map<String, Integer> compExecutors) {
            this.lastRequest = System.currentTimeMillis();
            this.compExecutors = compExecutors;
            this.topologyDetails = topologyDetails;
        }

        public AssignmentContext updateLastRebalance() {
            lastRebalance = System.currentTimeMillis();
            return this;
        }
    }

    private CuratorFramework zk;
    private String rootPath;
    private Map<String, AssignmentContext> watchingTopologies = new ConcurrentHashMap<>();
    private Nimbus.Client nimbus;
    private final int maxExecutorsPerWorker;
    private ExecutorService threadPool = Executors.newCachedThreadPool();

    public TopologyListener(Map<String, Object> conf) {
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                conf.get(Config.STORM_ZOOKEEPER_PORT));
        rootPath = (String) conf.getOrDefault(ResaConfig.ZK_ROOT_PATH, "/resa");
        checkZKNode();
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        maxExecutorsPerWorker = ConfigUtil.getInt(conf, ResaConfig.MAX_EXECUTORS_PER_WORKER, 10);
    }

    private void checkZKNode() {
        try {
            if (zk.checkExists().forPath(rootPath) == null) {
                zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootPath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Check root path failed: " + rootPath, e);
        }
    }

    public void synTopologies(Topologies topologies) {
        Set<String> aliveTopoIds = topologies.getTopologies().stream().map(TopologyDetails::getId)
                .collect(Collectors.toSet());
        // remove topologies that dead
        watchingTopologies.keySet().retainAll(aliveTopoIds);
        aliveTopoIds.stream().map(topologies::getById).forEach(topoDetails -> {
            String topoId = topoDetails.getId();
            // For a new joined topology, set a new watcher on it and add it to watching list
            // For a watching topology, update its running detail
            watchingTopologies.compute(topoId, (topologyId, context) ->
                    (context == null ? watchTopology(topoDetails) : context.setTopologyDetails(topoDetails)));
        });
    }

    /* add a watcher on zk to get a notification when a new assignment is set */
    private AssignmentContext watchTopology(TopologyDetails topoDetails) {
        // get current assignment and set a watcher on the zk node
        Map<String, Integer> compExecutors = getCompExecutorsAndWatch(topoDetails.getId());
        if (compExecutors == null) {
            return null;
        }
        LOG.info("Begin to watching topology " + topoDetails.getId());
        return new AssignmentContext(topoDetails, compExecutors);
    }

    private Map<String, Integer> getCompExecutorsAndWatch(String topoId) {
        String path = rootPath + '/' + topoId;
        try {
            byte[] data = zk.getData().usingWatcher(new TopologyWatcher(topoId)).forPath(path);
            if (data != null) {
                return (Map<String, Integer>) Utils.deserialize(data);
            }
        } catch (Exception e) {
        }
        return null;
    }

    /* zk watcher */
    private class TopologyWatcher implements Watcher {

        private String topoId;

        private TopologyWatcher(String topoId) {
            this.topoId = topoId;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDataChanged) {
                Map<String, Integer> newCompExecutors = getCompExecutorsAndWatch(topoId);
                if (newCompExecutors != null) {
                    watchingTopologies.computeIfPresent(topoId, (topoId, context) -> {
                        context.setCompExecutors(newCompExecutors);
                        // do this in the other thread, so that watchingTopologies can't block
                        threadPool.submit(() -> tryRebalance(topoId, context));
                        return context;
                    });
                }
                LOG.info("A new assignment is set for topology " + topoId);
            } else {
                // maybe something wrong with zk connection, force re-watch
                watchingTopologies.remove(topoId);
                LOG.warn("Receive a exception event for topology " + topoId + ", event type is "
                        + event.getType() + ", state is " + event.getState());
            }
        }

    }

    private void tryRebalance(String topoId, AssignmentContext context) {
        if (needRebalance(context)) {
            LOG.info("Trying rebalance for topology " + topoId);
            requestRebalance(topoId, context);
        } else {
            LOG.info("Request rebalance denied for topology " + topoId);
        }
    }

    /**
     * check whether a rebalance operation on the specified context is permitted
     */
    protected boolean needRebalance(AssignmentContext context) {
        return true;
    }

    private void requestRebalance(String topoId, AssignmentContext context) {
        int totolNumExecutors = context.compExecutors.values().stream().mapToInt(i -> i).sum();
        int numWorkers = totolNumExecutors / maxExecutorsPerWorker;
        if (totolNumExecutors % maxExecutorsPerWorker != 0) {
            numWorkers++;
        }
        RebalanceOptions options = new RebalanceOptions();
        //set rebalance options
        options.set_num_workers(numWorkers);
        options.set_num_executors(context.compExecutors);
        try {
            nimbus.rebalance(TopologyHelper.topologyId2Name(topoId), options);
            LOG.info("do rebalance successfully for topology " + topoId);
        } catch (Exception e) {
            LOG.warn("do rebalance failed for topology " + topoId, e);
        }
        context.updateLastRebalance();
    }

}