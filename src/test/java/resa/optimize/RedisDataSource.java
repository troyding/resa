package resa.optimize;

import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ding on 14-5-2.
 */
public class RedisDataSource {

    public static List<MeasuredData> readData(String host, int port, String queue) {
        ObjectMapper objectMapper = new ObjectMapper();
        Jedis jedis = new Jedis(host, port);
        List<MeasuredData> ret = new ArrayList<>();
        try {
            String line = null;
            while ((line = jedis.lpop(queue)) != null) {
                String[] tmp = line.split("->");
                String[] head = tmp[0].split(":");
                ret.add(new MeasuredData(head[0], Integer.valueOf(head[1]), System.currentTimeMillis(),
                        objectMapper.readValue(tmp[1], Map.class)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            jedis.disconnect();
        }
        return ret;
    }

}
