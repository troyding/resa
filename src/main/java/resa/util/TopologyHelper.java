package resa.util;

import backtype.storm.generated.*;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Helper class to get access online topology running details.
 *
 * @author Troy Ding
 */
public class TopologyHelper {

    /**
     * Get a running topology's details.
     *
     * @param topoName topology's name
     * @param conf     storm's conf, used to connect nimbus
     * @return null if topology is not exist, otherwise a TopologyDetails instance
     */
    public static TopologyDetails getTopologyDetails(String topoName, Map<String, Object> conf) {
        NimbusClient nimbusClient = NimbusClient.getConfiguredClient(conf);
        StormTopology topology;
        TopologyInfo topologyInfo;
        Map topologyConf;
        int numWorkers;
        try {
            Nimbus.Client nimbus = nimbusClient.getClient();
            ClusterSummary cluster = nimbus.getClusterInfo();
            Optional<TopologySummary> topo = cluster.get_topologies().stream()
                    .filter(e -> e.get_name().equals(topoName)).findFirst();
            if (!topo.isPresent()) {
                return null;
            }
            String topoId = topo.get().get_id();
            numWorkers = topo.get().get_num_workers();
            topology = nimbus.getUserTopology(topoId);
            topologyInfo = nimbus.getTopologyInfo(topoId);
            topologyConf = (Map) JSONValue.parse(nimbus.getTopologyConf(topoId));
        } catch (NotAliveException | TException e) {
            return null;
        } finally {
            nimbusClient.close();
        }
        Map<ExecutorDetails, String> exe2Components = topologyInfo.get_executors().stream()
                .collect(Collectors.toMap(e -> toExecutorDetails(e.get_executor_info()), e -> e.get_component_id()));
        return new TopologyDetails(topologyInfo.get_id(), topologyConf, topology, numWorkers, exe2Components);
    }

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @param ignoreSystemComp whether system component should be ignore
     * @return
     */
    public static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails, boolean ignoreSystemComp) {
        Predicate<Map.Entry<ExecutorDetails, String>> p = null;
        if (ignoreSystemComp) {
            p = e -> !Utils.isSystemId(e.getValue());
        }
        return componentToTasks(topoDetails, p);
    }

    /**
     * Get bolt component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    public static Map<String, List<Integer>> boltComponentToTasks(TopologyDetails topoDetails) {
        Set<String> bolts = topoDetails.getTopology().get_bolts().keySet();
        Predicate<Map.Entry<ExecutorDetails, String>> p = e -> bolts.contains(e.getValue());
        return componentToTasks(topoDetails, p);
    }

    /**
     * Get spout component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    public static Map<String, List<Integer>> spoutComponentToTasks(TopologyDetails topoDetails) {
        Set<String> spouts = topoDetails.getTopology().get_spouts().keySet();
        Predicate<Map.Entry<ExecutorDetails, String>> p = e -> spouts.contains(e.getValue());
        return componentToTasks(topoDetails, p);
    }

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    private static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails,
                                                               Predicate<Map.Entry<ExecutorDetails, String>> p) {
        Stream<Map.Entry<ExecutorDetails, String>> stream = topoDetails.getExecutorToComponent().entrySet().stream();
        if (p != null) {
            stream = stream.filter(p);
        }
        return stream.collect(Collectors.groupingBy(Map.Entry::getValue,
                        Collectors.mapping(e -> getTaskIds(e.getKey()),
                                Collector.of(ArrayList::new, (all, l) -> all.addAll(l),
                                        (all, l) -> {
                                            all.addAll(l);
                                            return all;
                                        }
                                )
                        )
                )
        );
    }

    /**
     * Get component tasks from TopologyDetails object
     *
     * @param topoDetails
     * @return
     */
    public static Map<String, List<Integer>> componentToTasks(TopologyDetails topoDetails) {
        return componentToTasks(topoDetails, false);
    }

    private static ExecutorDetails toExecutorDetails(ExecutorInfo executorInfo) {
        return new ExecutorDetails(executorInfo.get_task_start(), executorInfo.get_task_end());
    }

    public static List<Integer> getTaskIds(ExecutorDetails executorDetails) {
        int start = executorDetails.getStartTask();
        int end = executorDetails.getEndTask();
        return start == end ? Arrays.asList(start)
                : IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
    }


}
