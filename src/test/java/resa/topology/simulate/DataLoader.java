package resa.topology.simulate;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by ding on 14-7-29.
 */
public class DataLoader extends BaseBasicBolt {

    private CuratorFramework zk;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        zk = Utils.newCuratorStarted(stormConf, (List<String>) stormConf.get("storm.zookeeper.servers"),
                stormConf.get("storm.zookeeper.port"));
        Map<String, Number> toLoad = (Map<String, Number>) stormConf.get("taskToData");
        int count = toLoad.getOrDefault(String.valueOf(context.getThisTaskIndex()), 0).intValue();
        String resource = stormConf.get("resource").toString();
        long currTime = System.currentTimeMillis();
        if (count > 0) {
            List<Callable<Long>> tasks = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                tasks.add(() -> loadData(resource));
            }
            try {
                for (Future<Long> task : context.getSharedExecutor().invokeAll(tasks)) {
                    task.get();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {
            zk.create().creatingParentsIfNeeded()
                    .forPath("/resa/" + context.getStormId() + "/" + context.getThisComponentId() + "-"
                                    + context.getThisTaskIndex(),
                            String.valueOf(System.currentTimeMillis() - currTime).getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Long loadData(String url) throws IOException {
        Path f = Files.createTempFile("resa-", ".tmp");
        Files.copy(new URL(url).openStream(), f, StandardCopyOption.REPLACE_EXISTING);
        try {
            return Files.size(f);
        } finally {
            Files.delete(f);
        }
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
