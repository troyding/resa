package resa.util;

import backtype.storm.serialization.SerializationFactory;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * Created by ding on 14-7-31.
 */
public class KryoTest {

    @Test
    public void loadData() throws FileNotFoundException {
        Kryo kryo = SerializationFactory.getKryo(Utils.readStormConfig());
        Input in = new Input(new FileInputStream("/tmp/task-021.data"));
        System.out.println(in.readString());
        System.out.println(kryo.readClassAndObject(in).getClass());
    }

}
