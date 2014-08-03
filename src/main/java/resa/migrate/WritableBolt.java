package resa.migrate;

import backtype.storm.Config;
import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.topology.DelegatedBolt;
import resa.util.ConfigUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Created by ding on 14-6-7.
 */
public class WritableBolt extends DelegatedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(WritableBolt.class);
    private static final Map<Integer, Object> DATA_HOLDER = new ConcurrentHashMap<>();

    private transient Map<String, Object> conf;
    private transient Kryo kryo;
    private transient Map<String, Object> dataRef;
    private Path loaclDataPath;
    private transient ExecutorService threadPool;
    private int myTaskId;

    public WritableBolt(IRichBolt delegate) {
        super(delegate);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        this.conf = conf;
        this.myTaskId = context.getThisTaskId();
        kryo = SerializationFactory.getKryo(conf);
        LOG.info("Bolt " + getDelegate().getClass() + " need persist");
        dataRef = getDataRef(context);
        loaclDataPath = Paths.get((String) conf.get(Config.STORM_LOCAL_DIR), "data", context.getStormId());
        if (!Files.exists(loaclDataPath)) {
            try {
                Files.createDirectories(loaclDataPath);
            } catch (FileAlreadyExistsException e) {
            } catch (IOException e) {
                LOG.warn("Cannot create data path: " + loaclDataPath, e);
                loaclDataPath = null;
            }
        }
        if (loaclDataPath != null) {
            loaclDataPath = loaclDataPath.resolve(String.format("task-%03d.data", context.getThisTaskId()));
        }
        seekAndLoadTaskData(context.getStormId(), context.getThisTaskId());
        int checkpointInt = ConfigUtil.getInt(conf, "resa.comp.checkpoint.interval.sec", 30);
        if (checkpointInt > 0) {
            context.registerMetric("serialized", () -> createCheckpointAndGetSize(context), checkpointInt);
        }
        this.threadPool = context.getSharedExecutor();
        super.prepare(conf, context, outputCollector);
    }

    private void seekAndLoadTaskData(String topoId, int taskId) {
        /* load data:
           1. find in memory, maybe data migration was not required
           2. TODO: retrieve data from src worker using TCP
           3. try to load checkpoint from redis or other distributed systems
        */
        Object data = DATA_HOLDER.remove(taskId);
        if (data != null) {
            dataRef.putAll((Map<String, Object>) data);
        } else {
            // load checkpoint from redis, create a new Interface later to avoid hard code
            String queueName = topoId;
            queueName = String.format("%s-%03d-data", queueName, taskId);
            InputStream dataSource = RedisInputStream.create((String) conf.get("redis.host"),
                    ConfigUtil.getInt(conf, "redis.port", 6379), queueName);
            if (dataSource != null) {
                try {
                    loadData(dataSource);
                } catch (Exception e) {
                    LOG.warn("load data from " + dataSource + " failed, localDataPath=" + loaclDataPath);
                }
            }
        }
    }

    private static Map<String, Object> getDataRef(TopologyContext context) {
        try {
            Field f = context.getClass().getDeclaredField("_taskData");
            f.setAccessible(true);
            return (Map<String, Object>) f.get(context);
        } catch (Exception e) {
        }
        return Collections.EMPTY_MAP;
    }

    private Long createCheckpointAndGetSize(TopologyContext context) {
        Long size = -1L;
        if (!dataRef.isEmpty() && loaclDataPath != null) {
            try {
                size = writeData(Files.newOutputStream(loaclDataPath));
                String queueName = String.format("%s-%03d-data", context.getStormId(), context.getThisTaskId());
                threadPool.submit(() -> writeData(new RedisOutputStream((String) conf.get("redis.host"),
                        ConfigUtil.getInt(conf, "redis.port", 6379), queueName)));
            } catch (IOException e) {
                LOG.warn("Save bolt failed", e);
            }
        }
        return size;
    }

    private void loadData(InputStream in) {
        Input kryoIn = new Input(in);
        int size = kryoIn.readInt();
        for (int i = 0; i < size; i++) {
            dataRef.put(kryoIn.readString(), kryo.readClassAndObject(kryoIn));
        }
        kryoIn.close();
    }

    private long writeData(OutputStream out) {
        Output kryoOut = new Output(out);
        kryoOut.writeInt(dataRef.size());
        dataRef.forEach((k, v) -> {
            kryoOut.writeString(k);
            kryo.writeClassAndObject(kryoOut, v);
        });
        long size = kryoOut.total();
        kryoOut.close();
        return size;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (!dataRef.isEmpty()) {
            saveData();
        }
    }

    private void saveData() {
        String workerTasks = System.getProperty("new-worker-assignment");
        if (workerTasks != null && workerTasks.contains(String.valueOf(myTaskId))) {
            DATA_HOLDER.put(myTaskId, dataRef);
        } else if (loaclDataPath != null) {
            try {
                writeData(Files.newOutputStream(loaclDataPath));
            } catch (IOException e) {
                LOG.info("Save data failed", e);
            }
        }
    }

}
