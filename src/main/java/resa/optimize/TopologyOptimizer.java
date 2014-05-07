package resa.optimize;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import resa.metrics.MeasuredData;
import resa.util.ConfigUtil;
import resa.util.TopologyHelper;

import java.util.*;
import java.util.stream.Collectors;

import static resa.util.ResaConfig.*;

/**
 * Created by ding on 14-4-26.
 */
public class TopologyOptimizer {

    public static interface MeasuredSource {
        public Iterable<MeasuredData> retrieve();
    }

    private static final Logger LOG = Logger.getLogger(TopologyOptimizer.class);

    private final Timer timer = new Timer(true);
    private Map<String, Integer> currAllocation;
    private int maxExecutorsPerWorker;
    private int rebalanceWaitingSecs;
    private Nimbus.Client nimbus;
    private String topologyName;
    private String topologyId;
    private StormTopology rawTopology;
    private Map<String, Object> conf;
    private MeasuredSource measuredSource;
    private DecisionMaker decisionMaker;

    public void init(String topologyName, Map<String, Object> conf, MeasuredSource measuredSource) {
        this.conf = conf;
        this.topologyName = topologyName;
        this.measuredSource = measuredSource;
        maxExecutorsPerWorker = ConfigUtil.getInt(conf, MAX_EXECUTORS_PER_WORKER, 10);
        rebalanceWaitingSecs = ConfigUtil.getInt(conf, REBALANCE_WAITING_SECS, -1);
        // connected to nimbus
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        this.topologyId = TopologyHelper.getTopologyId(nimbus, topologyName);
        this.rawTopology = getRawTopologyThrow();
        // current allocation should be retrieved from nimbus
        currAllocation = getTopologyCurrAllocation();
        try {
            String defaultAanlyzer = SimpleModelDecisionMaker.class.getName();
            decisionMaker = Class.forName((String) conf.getOrDefault(ANALYZER_CLASS, defaultAanlyzer))
                    .asSubclass(DecisionMaker.class).newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Create Analyzer failed", e);
        }
        decisionMaker.init(conf, currAllocation, rawTopology);
    }

    public void start() {
        long calcInterval = ConfigUtil.getInt(conf, OPTIMIZE_INTERVAL, 30) * 1000;
        timer.scheduleAtFixedRate(new OptimizeTask(), calcInterval * 2, calcInterval);
        LOG.info(String.format("Init Topology Optimizer successfully with calc interval is %dms", calcInterval));
    }

    public void stop() {
        timer.cancel();
    }

    private class OptimizeTask extends TimerTask {

        @Override
        public void run() {
            Iterable<MeasuredData> data = measuredSource.retrieve();
            // get current ExecutorDetails from nimbus
            Map<String, List<ExecutorDetails>> topoExecutors = TopologyHelper.getTopologyExecutors(nimbus, topologyId);
            // topoExecutors == null means nimbus temporarily unreachable or this topology has been killed
            Map<String, Integer> allc = topoExecutors != null ? calcAllocation(topoExecutors) : null;
            if (allc != null && !allc.equals(currAllocation)) {
                LOG.info("Topology allocation changed");
                currAllocation = allc;
                // discard old MeasuredData
                consumeData(data);
                decisionMaker.allocationChanged(Collections.unmodifiableMap(currAllocation));
            } else {
                AggResultCalculator calculator = new AggResultCalculator(data, topoExecutors, rawTopology);
                calculator.calCMVStat();
                Map<String, Integer> newAllocation = calcNewAllocation(calculator.getResults());
                if (newAllocation != null && !newAllocation.equals(currAllocation)) {
                    LOG.info("Detected topology allocation changed, request rebalance....");
                    requestRebalance(newAllocation);
                }
            }
        }

        private void consumeData(Iterable<MeasuredData> data) {
            data.forEach((e) -> {
            });
        }

        private Map<String, Integer> calcAllocation(Map<String, List<ExecutorDetails>> topoExecutors) {
            return topoExecutors.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
        }

        private Map<String, Integer> calcNewAllocation(Map<String, AggResult[]> data) {
            int maxExecutors = Math.max(ConfigUtil.getInt(conf, Config.TOPOLOGY_WORKERS, 1),
                    getNumWorkers(currAllocation)) * maxExecutorsPerWorker;
            Map<String, Integer> decision = decisionMaker.make(data, maxExecutors);
            return decision;
        }
    }

    private StormTopology getRawTopologyThrow() {
        try {
            return nimbus.getUserTopology(topologyId);
        } catch (Exception e) {
            throw new RuntimeException("Get raw topology failed, id is " + topologyId, e);
        }
    }

    /* call nimbus to get current topology allocation */
    private Map<String, Integer> getTopologyCurrAllocation() {
        try {
            TopologyInfo topoInfo = nimbus.getTopologyInfo(topologyId);
            return topoInfo.get_executors().stream().filter(e -> !Utils.isSystemId(e.get_component_id()))
                    .collect(Collectors.groupingBy(e -> e.get_component_id(),
                            Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));
        } catch (Exception e) {
            LOG.warn("Get topology curr allocation from nimbus failed", e);
        }
        return Collections.emptyMap();
    }

    private int getNumWorkers(Map<String, Integer> allocation) {
        int totolNumExecutors = allocation.values().stream().mapToInt(Integer::intValue).sum();
        int numWorkers = totolNumExecutors / maxExecutorsPerWorker;
        if (totolNumExecutors % maxExecutorsPerWorker > (int) (maxExecutorsPerWorker / 2)) {
            numWorkers++;
        }
        return numWorkers;
    }

    /* Send rebalance request to nimbus */
    private boolean requestRebalance(Map<String, Integer> allocation) {
        int numWorkers = getNumWorkers(allocation);
        RebalanceOptions options = new RebalanceOptions();
        //set rebalance options
        options.set_num_workers(numWorkers);
        options.set_num_executors(allocation);
        if (rebalanceWaitingSecs >= 0) {
            options.set_wait_secs(rebalanceWaitingSecs);
        }
        try {
            nimbus.rebalance(topologyName, options);
            LOG.info("do rebalance successfully for topology " + topologyName);
            return true;
        } catch (Exception e) {
            LOG.warn("do rebalance failed for topology " + topologyName, e);
        }
        return false;
    }

}
