package resa.util;

import backtype.storm.serialization.SerializationFactory;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.junit.Test;
import resa.migrate.FileClient;

import java.util.Map;

/**
 * Created by ding on 14-7-31.
 */
public class KryoTest {

    @Test
    public void loadData() throws Exception {
        Kryo kryo = SerializationFactory.getKryo(Utils.readStormConfig());
        Input in = new Input(FileClient.openInputStream("192.168.0.19", 19888, "fpt-11-1407343473/task-013.data"));
        int size = in.readInt();
        System.out.println(in.readString());
        Class c = kryo.readClass(in).getType();
        Object v;
        if (KryoSerializable.class.isAssignableFrom(c)) {
            v = new DefaultSerializers.KryoSerializableSerializer().read(kryo, in, c);
        } else {
            v = kryo.readClassAndObject(in);
        }
        System.out.println(((Map) v).size());
    }

}
