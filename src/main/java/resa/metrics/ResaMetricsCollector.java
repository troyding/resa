package resa.metrics;

import backtype.storm.Config;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import resa.optimize.TopologyOptimizer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-5-5.
 */
public class ResaMetricsCollector extends FilteredMetricsCollector {

    private static final Map<String, String> METRICS_NAME_MAPPING = new HashMap<>();

    static {
        // add metric name mapping here
        METRICS_NAME_MAPPING.put("__sendqueue", MetricNames.SEND_QUEUE);
        METRICS_NAME_MAPPING.put("__receive", MetricNames.RECV_QUEUE);
        METRICS_NAME_MAPPING.put("complete-latency", MetricNames.COMPLETE_LATENCY);
        METRICS_NAME_MAPPING.put("execute", MetricNames.TASK_EXECUTE);
    }

    private int bufferSize = 100;
    private volatile List<MeasuredData> measureBuffer;
    private TopologyOptimizer topologyOptimizer = new TopologyOptimizer();

    @Override
    public void prepare(Map conf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        super.prepare(conf, arg, context, errorReporter);
        measureBuffer = new ArrayList<>(bufferSize);
        topologyOptimizer.init((String) conf.get(Config.TOPOLOGY_NAME), conf, this::getCachedDataAndClearBuffer);
        topologyOptimizer.start();
    }

    private List<MeasuredData> getCachedDataAndClearBuffer() {
        List<MeasuredData> ret = measureBuffer;
        measureBuffer = new ArrayList<>(bufferSize);
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
        measureBuffer.add(new MeasuredData(taskInfo.srcComponentId, taskInfo.srcTaskId, taskInfo.timestamp, ret));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        topologyOptimizer.stop();
    }
}
