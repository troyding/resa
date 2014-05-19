package resa.metrics;

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Class used to filter useless metrics that collected by storm build-in metric system.
 *
 * @author Troy Ding
 */
public abstract class FilteredMetricsCollector implements IMetricsConsumer {

    public static final String APPROVED_METRIC_NAMES = "resa.metric.approved.names";

    private final Map<String, String> approvedMetricNames = new HashMap<>();

    @Override
    public void prepare(Map stormConf, Object arg, TopologyContext context, IErrorReporter errorReporter) {
        List<String> metricNames = (List<String>) stormConf.get(APPROVED_METRIC_NAMES);
        if (metricNames != null) {
            metricNames.stream().forEach((line) -> {
                line = line.trim();
                if (line.isEmpty()) {
                    return;
                }
                String[] tmp = line.split("\\s+");
                if (tmp.length == 1) {
                    addApprovedMetirc(tmp[0]);
                } else {
                    addApprovedMetirc(tmp[0], tmp[1]);
                }
            });
        }
    }

    public void addApprovedMetirc(String metricName, String alias) {
        approvedMetricNames.put(metricName, alias);
    }

    public void addApprovedMetirc(String metricName) {
        addApprovedMetirc(metricName, metricName);
    }

    public void removeApprovedMetirc(String metricName) {
        approvedMetricNames.remove(metricName);
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        //check task fist
        if (keepTask(taskInfo)) {
            return;
        }
        // select points that need to keep based on the implementation of keepPoint function
        List<DataPoint> selectedPoints = dataPoints.stream().map(dataPoint -> processPoint(taskInfo, dataPoint))
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (!selectedPoints.isEmpty()) {
            handleSelectedDataPoints(taskInfo, selectedPoints);
        }
    }

    /**
     * Check whether this task metrics should be keep, default implementations filter
     * metrics that generated by system components.
     *
     * @param taskInfo
     * @return true if this metrics should be keep; otherwise false
     */
    protected boolean keepTask(TaskInfo taskInfo) {
        //ignore system component
        return Utils.isSystemId(taskInfo.srcComponentId);
    }

    private DataPoint processPoint(TaskInfo taskInfo, DataPoint dataPoint) {
        String alias = approvedMetricNames.get(dataPoint.name);
        return alias != null && keepPoint(taskInfo, dataPoint) ? new DataPoint(alias, dataPoint.value) : null;
    }

    /**
     * Check whether this data point should be keep, default implementations filters empty collections
     *
     * @param dataPoint
     * @return false if this data point should be removed
     */
    protected boolean keepPoint(TaskInfo taskInfo, DataPoint dataPoint) {
        Object value = dataPoint.value;
        if (value == null) {
            return false;
        } else if (value instanceof Map) {
            return !((Map) value).isEmpty();
        } else if (value instanceof Collection) {
            return !((Collection) value).isEmpty();
        }
        return true;
    }

    protected abstract void handleSelectedDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints);

    @Override
    public void cleanup() {

    }
}
