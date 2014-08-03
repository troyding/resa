package resa.util;

import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by ding on 14-7-31.
 */
public class ReadAssignmentTest {

    @Test
    public void read() throws Exception {
        CuratorFramework zk = Utils.newCuratorStarted(Utils.readStormConfig(), Arrays.asList("localhost"), 2181);
        byte[] data = zk.getData().forPath("/storm/workerbeats/wc-1-1406881285/ea967da2-7293-4a68-983f-34872d89e132-6700");
        Object assignment = Utils.deserialize(data);
        System.out.println(assignment);
    }

}
