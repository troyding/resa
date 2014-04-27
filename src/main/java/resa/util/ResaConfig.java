package resa.util;

import backtype.storm.Config;
import backtype.storm.utils.Utils;
import resa.optimize.TopologyOptimizer;

import java.util.Map;

/**
 * Created by ding on 14-4-26.
 */
public class ResaConfig extends Config {

    public static final String TRACE_COMP_QUEUE = "topology.queue.trace";

    public static final String COMP_QUEUE_SAMPLE_RATE = "resa.comp.queue.sample.rate";

    public static final String COMP_SAMPLE_RATE = "resa.comp.sample.rate";

    public static final String MAX_EXECUTORS_PER_WORKER = "resa.scheduler.max.executor.per.worker";

    public static final String ZK_ROOT_PATH = "resa.scheduler.zk.root";

    private ResaConfig(boolean loadDefault) {
        if (loadDefault) {
            //read default.yaml & storm.yaml
            try {
                putAll(Utils.readStormConfig());
            } catch (Throwable e) {
            }
        }
        Map<String, Object> conf = Utils.findAndReadConfigFile("resa.yaml", false);
        if (conf != null) {
            putAll(conf);
        }
    }

    /**
     * Create a new Conf, then load default.yaml and storm.yaml
     *
     * @return
     */
    public static ResaConfig create() {
        return create(true);
    }

    /**
     * Create a new resa Conf
     *
     * @param loadDefault
     * @return
     */
    public static ResaConfig create(boolean loadDefault) {
        return new ResaConfig(loadDefault);
    }

    public void addOptimizerSupport() {
        registerMetricsConsumer(TopologyOptimizer.class, 1);
    }
}
