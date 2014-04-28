package resa.scheduler;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.utils.NimbusClient;
import org.apache.thrift7.TException;

import java.util.Map;

/**
 * Created by ding on 14-4-28.
 */
public class HealthyChecker extends Thread {

    private Nimbus.Client nimbus;

    public HealthyChecker(Map<String, Object> conf) {
        nimbus = NimbusClient.getConfiguredClient(conf).getClient();
    }

    private boolean needMoreWorker() {
        ClusterSummary clusterSummary;
        try {
            clusterSummary = nimbus.getClusterInfo();
        } catch (TException e) {
            return false;
        }
        int numFreeWorkers = clusterSummary.get_supervisors().stream()
                .mapToInt(s -> s.get_num_workers() - s.get_num_used_workers()).sum();
        return numFreeWorkers == 0;
    }

    @Override
    public void run() {
        while (true) {
            if (needMoreWorker()) {
                //add more worker
            }
        }
    }
}
