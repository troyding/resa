package resa.migrate;

import backtype.storm.serialization.SerializationFactory;
import backtype.storm.task.IBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicBoltExecutor;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.topology.DelegatedBolt;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

/**
 * Created by ding on 14-6-7.
 */
public class WritableBolt extends DelegatedBolt {

    private static final Logger LOG = LoggerFactory.getLogger(WritableBolt.class);

    private transient Map<String, Object> conf;
    private transient Kryo kryo;
    private transient Persistable persistableBolt;

    public WritableBolt(IRichBolt delegate) {
        super(delegate);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        super.prepare(conf, context, outputCollector);
        this.conf = conf;
        // find the last bolt, and check whether it is persistable
        persistableBolt = getPersistableObject();
        if (persistableBolt != null) {
            kryo = SerializationFactory.getKryo(conf);
            LOG.info("Bolt " + persistableBolt.getClass() + " support persist");
            loadData();
        } else {
            kryo = null;
            LOG.info("No persistable bolt is found");
        }
        context.registerMetric("serialized", this::getSerializedSize, 30);
    }

    private Persistable getPersistableObject() {
        IBolt bolt = this.getDelegate();
        // find the last bolt
        while (bolt instanceof DelegatedBolt) {
            if (bolt instanceof Persistable) {
                break;
            }
            bolt = ((DelegatedBolt) bolt).getDelegate();
        }
        if (bolt instanceof Persistable) {
            return (Persistable) bolt;
        } else if (bolt instanceof BasicBoltExecutor) {
            try {
                Field f = BasicBoltExecutor.class.getDeclaredField("_bolt");
                f.setAccessible(true);
                IBasicBolt basicBolt = (IBasicBolt) f.get(bolt);
                return basicBolt instanceof Persistable ? (Persistable) basicBolt : null;
            } catch (Exception e) {
                return null;
            }
        }
        return null;
    }

    private Long getSerializedSize() {
        Long size = null;
        if (persistableBolt != null) {
            Path file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
            try (Output out = new Output(Files.newOutputStream(file))) {
                persistableBolt.save(kryo, out);
            } catch (IOException e) {
                LOG.warn("Save bolt failed", e);
            }
            try {
                size = Files.size(file);
                Files.deleteIfExists(file);
            } catch (IOException e) {
            }
        }
        return size;
    }

    private void loadData() {
        try {
            persistableBolt.read(SerializationFactory.getKryo(conf), null);
        } catch (IOException e) {
            LOG.warn("Load bolt failed", e);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (persistableBolt != null) {
            try {
                persistableBolt.save(kryo, null);
            } catch (IOException e) {
                LOG.warn("Save bolt failed", e);
            }
        }
    }
}
