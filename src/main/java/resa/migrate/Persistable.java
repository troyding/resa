package resa.migrate;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;

/**
 * Created by ding on 14-6-8.
 */
public interface Persistable {

    public void save(Kryo kryo, Output output) throws IOException;

    public void read(Kryo kryo, Input input) throws IOException;

}
