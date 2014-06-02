package resa.tools;

import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import resa.util.ConfigUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Created by ding on 14-3-18.
 */
public class DataSenderWithTS {

    private String host;
    private int port;
    private String queueName;
    private int maxPaddingSize;

    public DataSenderWithTS(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = (String) conf.get("redis.queue");
    }

    public void send2Queue(Path inputFile, LongSupplier sleep) throws IOException {
        Jedis jedis = new Jedis(host, port);
        AtomicLong counter = new AtomicLong(0);
        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            String line = null;
            while (line != null || (line = reader.readLine()) != null) {
                long ms = sleep.getAsLong();
                if (ms > 0) {
                    Utils.sleep(ms);
                }
                if (jedis.llen(queueName) < maxPaddingSize) {
                    String data = counter.getAndIncrement() + "|" + System.currentTimeMillis() + "|" + line;
                    jedis.rpush(queueName, data);
                    line = null;
                }
            }
        } finally {
            jedis.quit();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("usage: DataSender <confFile> <inputFile> <maxPadding Size> " +
                    "[-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
            return;
        }
        DataSenderWithTS sender = new DataSenderWithTS(ConfigUtil.readConfig(new File(args[0])));
        System.out.println("start sender");
        Path dataFile = Paths.get(args[1]);
        int maxPadding = Integer.parseInt(args[2]);
        sender.maxPaddingSize = maxPadding > 0 ? maxPadding : Integer.MAX_VALUE;
        switch (args[3].substring(1)) {
            case "deter":
                long sleep = (long) (1000 / Float.parseFloat(args[4]));
                sender.send2Queue(dataFile, () -> sleep);
                break;
            case "poison":
                double lambda = Float.parseFloat(args[4]);
                sender.send2Queue(dataFile, () -> (long) (-Math.log(Math.random()) * 1000 / lambda));
                break;
            case "uniform":
                if (args.length < 5) {
                    System.out.println("usage: DataSender <confFile> <inputFile> [-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
                    return;
                }
                double left = Float.parseFloat(args[4]);
                double right = Float.parseFloat(args[5]);
                sender.send2Queue(dataFile, () -> (long) (1000 / (Math.random() * (right - left) + left)));
            default:
                System.out.println("usage: DataSender <confFile> <inputFile> [-deter <rate>] [-poison <lambda>] [-uniform <left> <right>]");
                ///sender.send2Queue(dataFile, () -> 0);
                break;
        }
        System.out.println("end sender");
    }

}
