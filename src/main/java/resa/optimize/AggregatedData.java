package resa.optimize;

import backtype.storm.task.GeneralTopologyContext;
import resa.util.FixedSizeQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-4-30.
 */
class AggregatedData {

    public AggregatedData(GeneralTopologyContext topologyContext, int historySize) {
        this.topologyContext = topologyContext;
        this.historySize = historySize;
    }

    private GeneralTopologyContext topologyContext;
    private int historySize;
    public final Map<Integer, ComponentAggResult> taskResult = new HashMap<>();
    public final Map<String, Queue<ComponentAggResult>> compHistoryResults = new HashMap<>();

    public void putResult(int task, ComponentAggResult taskAggResult) {
        taskResult.put(task, taskAggResult);
    }

    public void rotate() {
        // group measured data by component name
        Map<String, List<ComponentAggResult>> groupedResult = taskResult.entrySet().stream()
                .collect(Collectors.groupingBy(e -> topologyContext.getComponentId(e.getKey()),
                        Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
        //calc each component's aggregate result and add it to history list
        groupedResult.forEach((comp, results) -> {
            ComponentAggResult.ComponentType t = topologyContext.getRawTopology().get_spouts().containsKey(comp) ?
                    ComponentAggResult.ComponentType.SPOUT : ComponentAggResult.ComponentType.BOLT;
            compHistoryResults.computeIfAbsent(comp, (k) -> new FixedSizeQueue(historySize))
                    .add(ComponentAggResult.getCombinedResult(results, t));
        });
    }

}
