package resa.migrate;

import junit.framework.TestCase;

public class RedisOutputStreamTest extends TestCase {

    public void testWrite() throws Exception {

        String host = "192.168.0.30";
        int port = 6379;
        String queueName = "tom-test";
        boolean overwrite = true;
        int chunkSize = 4;


        String testOutput = "aaaaabbbbbcccccdddddeeeeefffff";
        byte[] bTestOutput = testOutput.getBytes();

        try(RedisOutputStream ros = new RedisOutputStream(host, port, queueName, overwrite, chunkSize)) {
            ros.write(bTestOutput);
        }
    }
}