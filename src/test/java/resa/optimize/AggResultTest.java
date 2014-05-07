package resa.optimize;

import org.junit.Test;

public class AggResultTest {

    private AggResult aggResult = new AggResult(AggResult.ComponentType.BOLT);

    @Test
    public void testGetSimpleCombinedProcessedTuple() throws Exception {
        CntMeanVar cmv = aggResult.getSimpleCombinedProcessedTuple();
        System.out.println(cmv);
    }
}