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

import java.io.IOException;
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

    public WritableBolt(IRichBolt delegate) {
        super(delegate);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        super.prepare(conf, context, outputCollector);
        this.conf = conf;
        IRichBolt bolt = super.getDelegate();
        if (bolt instanceof Persistable) {
            kryo = SerializationFactory.getKryo(conf);
            loadData((Persistable) bolt);
        } else {
            kryo = null;
        }
        context.registerMetric("serialized", this::getSerializedSize, 300);
    }

    private Long getSerializedSize() {
        Long size = null;
        if (kryo != null) {
            Path file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
            try (Output out = new Output(Files.newOutputStream(file))) {
                ((Persistable) super.getDelegate()).save(kryo, out);
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

    private void loadData(Persistable bolt) {
        try {
            bolt.read(SerializationFactory.getKryo(conf), null);
        } catch (IOException e) {
            LOG.warn("Load bolt failed", e);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (kryo != null) {
            try {
                ((Persistable) super.getDelegate()).save(kryo, null);
            } catch (IOException e) {
                LOG.warn("Save bolt failed", e);
            }
        }
    }
}
