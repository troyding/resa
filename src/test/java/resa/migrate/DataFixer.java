package resa.migrate;

import backtype.storm.serialization.SerializationFactory;
import backtype.storm.utils.Utils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import resa.topology.fp.Detector;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Created by ding on 14-9-8.
 */
public class DataFixer {

    @Test
    public void fix() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path dir = new Path("/resa/fpt-100");
        String[] files = Stream.of(fs.listStatus(dir)).map(s -> s.getPath().getName()).sorted().toArray(String[]::new);
        double[] dataSize = Files.readAllLines(Paths.get("/Volumes/Data/work/doctor/resa/exp/fp-data-sizes-064-100.txt"))
                .stream().map(String::trim).filter(s -> !s.isEmpty()).mapToDouble(Double::valueOf)
                .map(d -> d * 1024 * 1024).toArray();
        if (files.length != dataSize.length) {
            throw new IllegalArgumentException("size mismatch");
        }
        System.out.println("files: " + Arrays.toString(files));
        IntStream.range(0, dataSize.length).parallel().forEach(i -> {
            try {
                FileStatus fileStatus = fs.getFileStatus(new Path(dir, files[i]));
                long maxSize = Math.max(fileStatus.getLen(), (long) dataSize[i]);
                if (Math.abs(fileStatus.getLen() - (long) dataSize[i]) > (long) (0.1 * maxSize)) {
                    System.out.println("Reduce file " + files[i] + " from " + fileStatus.getLen() + " to " + dataSize[i]);
                    fs.rename(new Path(dir, files[i]), new Path(dir, files[i] + ".bak"));
                    double percentage = dataSize[i] / fileStatus.getLen();
                    copy(fs.open(new Path(dir, files[i] + ".bak")), fs.create(new Path(dir, files[i]), true,
                            4 * 1024 * 1024, fileStatus.getReplication(), fileStatus.getBlockSize()), percentage);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void copy(InputStream in, OutputStream out, double percentage) {
        Kryo kryo = SerializationFactory.getKryo(Utils.readStormConfig());
        Input input = new Input(in);
        Output output = new Output(out);
        output.writeInt(input.readInt());
        output.writeString(input.readString());
        Class c = kryo.readClass(input).getType();
        kryo.writeClass(output, c);
        Detector.PatternDB db = (Detector.PatternDB) new DefaultSerializers.KryoSerializableSerializer()
                .read(kryo, input, c);
        int count = (int) (db.size() * (1.0 - percentage));
        for (Iterator<?> iter = db.entrySet().iterator(); iter.hasNext(); ) {
            if (count-- <= 0) {
                break;
            }
            iter.next();
            iter.remove();
        }
        db.write(kryo, output);
        input.close();
        output.close();
    }

}
