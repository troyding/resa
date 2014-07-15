package resa.migrate;

import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.topology.DelegatedBolt;
import resa.util.ConfigUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Created by ding on 14-6-7.
 */
public class WritableBolt extends DelegatedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(WritableBolt.class);

    private transient Map<String, Object> conf;
    private transient Kryo kryo;
    private transient Map<String, Object> dataRef;

    public WritableBolt(IRichBolt delegate) {
        super(delegate);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        this.conf = conf;
        kryo = SerializationFactory.getKryo(conf);
        LOG.info("Bolt " + getDelegate().getClass() + " need persist");
        dataRef = getDataRef(context);
        loadData();
        int checkpointInt = ConfigUtil.getInt(conf, "resa.comp.checkpoint.interval.sec", 30);
        if (checkpointInt > 0) {
            context.registerMetric("serialized", this::createCheckpointAndGetSize, checkpointInt);
        }
        super.prepare(conf, context, outputCollector);
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
        Long size = null;
        if (!dataRef.isEmpty()) {
            Path file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
            try (Output out = new Output(Files.newOutputStream(file))) {
                kryo.writeObject(out, dataRef);
                size = Files.size(file);
                Files.deleteIfExists(file);
            } catch (IOException e) {
                LOG.warn("Save bolt failed", e);
            }
        }
        return size;
    }

    private void loadData() {
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (!dataRef.isEmpty()) {
            kryo.writeObject(null, dataRef);
        }
    }
}
