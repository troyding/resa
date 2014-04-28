package resa.optimize;

import backtype.storm.Config;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;
import resa.metrics.FilteredMetricsCollector;
import resa.metrics.MetricNames;
import resa.util.ConfigUtil;
import resa.util.ResaConfig;

import java.util.*;
import java.util.stream.Collectors;

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

    private CuratorFramework zk;
    private volatile List<MeasuredData> metricsPool = new ArrayList<>(100);
    private String zkNode;
    private final Timer timer = new Timer(true);
    private Map<String, Integer> lastAllocation = new HashMap<>();

    @Override
    public void prepare(Map conf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        super.prepare(conf, arg, context, errorReporter);
        zk = Utils.newCuratorStarted(conf, (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS),
                conf.get(Config.STORM_ZOOKEEPER_PORT));
        zkNode = conf.getOrDefault(ResaConfig.ZK_ROOT_PATH, "/resa") + "/" + context.getStormId();
        try {
            if (zk.checkExists().forPath(zkNode) == null) {
                zk.create().creatingParentsIfNeeded().forPath(zkNode, Utils.serialize(lastAllocation));
            } else {
                zk.setData().forPath(zkNode, Utils.serialize(lastAllocation));
            }
        } catch (Exception e) {
            LOG.warn("check znode for topology failed: " + context.getStormId(), e);
            errorReporter.reportError(e);
        }
        long calcInterval = ConfigUtil.getInt(conf, ResaConfig.OPTIMIZE_INTERVAL, 30) * 1000;
        timer.schedule(new AllocationCalc(), calcInterval, calcInterval);
        LOG.info(String.format("Init Topology Optimizer successfully for %s:%d, calc interval is %dms",
                context.getThisComponentId(), context.getThisTaskId(), calcInterval));
    }

    private class AllocationCalc extends TimerTask {

        @Override
        public void run() {
            List<MeasuredData> data = getCachedDataAndClearBuffer();
            Map<String, Integer> newAllocation = calcNewAllocation(data);
            if (newAllocation != null && !newAllocation.equals(lastAllocation)) {
                try {
                    zk.setData().forPath(zkNode, Utils.serialize(newAllocation));
                    lastAllocation = newAllocation;
                } catch (Exception e) {
                    LOG.warn("set new allocation on znode '" + zkNode + "' fail", e);
                }
            }
        }

    }

    private Map<String, Integer> calcNewAllocation(List<MeasuredData> data) {
        return null;
    }

    private List<MeasuredData> getCachedDataAndClearBuffer() {
        List<MeasuredData> ret = metricsPool;
        metricsPool = new ArrayList<>(100);
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
        //add to cache
        metricsPool.add(new MeasuredData(taskInfo.srcComponentId, taskInfo.srcTaskId, taskInfo.timestamp, ret));
    }

}
