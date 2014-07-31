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

/**
 * Created by ding on 14-6-7.
 */
public class WritableBolt extends DelegatedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(WritableBolt.class);

    private transient Map<String, Object> conf;
    private transient Kryo kryo;
    private transient Map<String, Object> dataRef;
    private Path loaclDataPath;

    public WritableBolt(IRichBolt delegate) {
        super(delegate);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        this.conf = conf;
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
            context.registerMetric("serialized", this::createCheckpointAndGetSize, checkpointInt);
        }
        super.prepare(conf, context, outputCollector);
    }

    private void seekAndLoadTaskData(String topoId, int taskId) {
        InputStream dataSource = null;
        if (loaclDataPath != null && Files.exists(loaclDataPath)) {
            try {
                dataSource = Files.newInputStream(loaclDataPath);
            } catch (IOException e) {
            }
        } else {
            String queueName = topoId;
            queueName = String.format("%s-%03d-data", queueName, taskId);
            dataSource = RedisInputStream.create((String) conf.get("redis.host"),
                    ConfigUtil.getInt(conf, "redis.port", 6379), queueName);
        }
        if (dataSource != null) {
            loadData(dataSource);
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

    private Long createCheckpointAndGetSize() {
        Long size = -1L;
        if (!dataRef.isEmpty() && loaclDataPath != null) {
            try {
                writeData(Files.newOutputStream(loaclDataPath));
                size = Files.size(loaclDataPath);
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

    private void writeData(OutputStream out) {
        Output kryoOut = new Output(out);
        kryoOut.writeInt(dataRef.size());
        dataRef.forEach((k, v) -> {
            kryoOut.writeString(k);
            kryo.writeClassAndObject(kryoOut, v);
        });
        kryoOut.close();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (!dataRef.isEmpty()) {
            writeData(null);
        }
    }
}
