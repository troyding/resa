package resa.optimize;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.NimbusClient;
import org.apache.log4j.Logger;
import resa.metrics.FilteredMetricsCollector;
import resa.metrics.MetricNames;
import resa.optimize.MeasuredData.ComponentType;
import resa.util.ConfigUtil;
import resa.util.TopologyHelper;

import java.util.*;
import java.util.stream.Collectors;

import static resa.util.ResaConfig.*;

/**
 * Created by ding on 14-4-26.
 */
public class TopologyOptimizer extends FilteredMetricsCollector {

    private static final Map<String, String> METRICS_NAME_MAPPING = new HashMap<>();

    static {
        // add metric name mapping here
        METRICS_NAME_MAPPING.put("__sendqueue", MetricNames.SEND_QUEUE);
        METRICS_NAME_MAPPING.put("__receive", MetricNames.RECV_QUEUE);
        METRICS_NAME_MAPPING.put("complete-latency", MetricNames.COMPLETE_LATENCY);
        METRICS_NAME_MAPPING.put("execute", MetricNames.TASK_EXECUTE);
    }

    private static final Logger LOG = Logger.getLogger(TopologyOptimizer.class);

    private volatile List<MeasuredData> measureBuffer = new ArrayList<>(100);
    private final Timer timer = new Timer(true);
    private Map<String, Integer> currAllocation;
    private int maxExecutorsPerWorker;
    private int rebalanceWaitingSecs;
    private Nimbus.Client nimbus;
    private String topologyName;
    private GeneralTopologyContext topologyContext;
    private Map<String, Object> conf;

    @Override
    public void prepare(Map conf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        super.prepare(conf, arg, context, errorReporter);
        this.conf = conf;
        this.topologyContext = context;
        this.topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        maxExecutorsPerWorker = ConfigUtil.getInt(conf, MAX_EXECUTORS_PER_WORKER, 10);
        rebalanceWaitingSecs = ConfigUtil.getInt(conf, REBALANCE_WAITING_SECS, -1);
        // connected to nimbus
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
        // current allocation should be retrieved from nimbus
        currAllocation = getTopologyCurrAllocation();
        long calcInterval = ConfigUtil.getInt(conf, OPTIMIZE_INTERVAL, 30) * 1000;
        timer.schedule(new OptimizeTask(), calcInterval * 2, calcInterval);
        LOG.info(String.format("Init Topology Optimizer successfully for %s:%d, calc interval is %dms",
                context.getThisComponentId(), context.getThisTaskId(), calcInterval));
    }

    private class OptimizeTask extends TimerTask {

        private OptimizeAnalyzer optAnalyzer;

        OptimizeTask() {
            try {
                String defaultAanlyzer = SimpleModelOptimizeAnalyzer.class.getName();
                optAnalyzer = Class.forName((String) conf.getOrDefault(ANALYZER_CLASS, defaultAanlyzer))
                        .asSubclass(OptimizeAnalyzer.class).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Create Analyzer failed", e);
            }
            optAnalyzer.init(conf, topologyContext);
        }

        @Override
        public void run() {
            List<MeasuredData> data = getCachedDataAndClearBuffer();
            Map<String, Integer> newAllocation = calcNewAllocation(data);
            if (newAllocation != null && !newAllocation.equals(currAllocation)) {
                LOG.info("Detected topology allocation changed, request rebalance....");
                if (requestRebalance(topologyName, newAllocation)) {
                    currAllocation = newAllocation;
                }
            }
        }

        private Map<String, Integer> calcNewAllocation(List<MeasuredData> data) {
            OptimizeDecision decision = optAnalyzer.analyze(data,
                    Math.max(ConfigUtil.getInt(conf, Config.TOPOLOGY_WORKERS, 0),
                            getNumWorkers(currAllocation)), currAllocation
            );
            if (decision == null) {
                return null;
            } else if (decision.status == OptimizeDecision.Status.INFEASIBLE) {
                return decision.currOptAllocation;
            } else {
                return decision.minReqOptAllocation;
            }
        }
    }


    private List<MeasuredData> getCachedDataAndClearBuffer() {
        List<MeasuredData> ret = measureBuffer;
        measureBuffer = new ArrayList<>(100);
        return ret;
    }

    @Override
    protected boolean keepPoint(TaskInfo taskInfo, DataPoint dataPoint) {
        return METRICS_NAME_MAPPING.containsKey(dataPoint.name) && super.keepPoint(taskInfo, dataPoint);
    }

    @Override
    protected void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        Map<String, Object> ret = dataPoints.stream().collect(
                Collectors.toMap(p -> METRICS_NAME_MAPPING.get(p.name), p -> p.value));
        ComponentType t = topologyContext.getRawTopology().get_spouts().containsKey(taskInfo.srcComponentId)
                ? ComponentType.SPOUT : ComponentType.BOLT;
        //add to cache
        measureBuffer.add(new MeasuredData(t, taskInfo.srcComponentId, taskInfo.srcTaskId, taskInfo.timestamp, ret));
    }

    /* call nimbus to get current topology allocation */
    private Map<String, Integer> getTopologyCurrAllocation() {
        try {
            TopologyInfo topoInfo = nimbus.getTopologyInfo(topologyContext.getStormId());
            return topoInfo.get_executors().stream().collect(Collectors.groupingBy(e -> e.get_component_id(),
                    Collectors.reducing(0, e -> 1, (i1, i2) -> i1 + i2)));
        } catch (Exception e) {
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
    private boolean requestRebalance(String topoName, Map<String, Integer> allocation) {
        int numWorkers = getNumWorkers(allocation);
        RebalanceOptions options = new RebalanceOptions();
        //set rebalance options
        options.set_num_workers(numWorkers);
        options.set_num_executors(allocation);
        if (rebalanceWaitingSecs >= 0) {
            options.set_wait_secs(rebalanceWaitingSecs);
        }
        try {
            nimbus.rebalance(TopologyHelper.topologyId2Name(topoName), options);
            LOG.info("do rebalance successfully for topology " + topoName);
            return true;
        } catch (Exception e) {
            LOG.warn("do rebalance failed for topology " + topoName, e);
        }
        return false;
    }

}
