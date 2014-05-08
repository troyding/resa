package resa.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ding on 14-5-8.
 */
public class LogTest {

    @Test
    public void testLog() {
        Logger logger = LoggerFactory.getLogger(LogTest.class);
        logger.debug("debug");
        logger.info("info");
        logger.warn("warn");
    }

}
