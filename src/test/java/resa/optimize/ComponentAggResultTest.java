package resa.optimize;

import org.junit.Test;

public class ComponentAggResultTest {

    private ComponentAggResult componentAggResult = new ComponentAggResult(MeasuredData.ComponentType.BOLT);

    @Test
    public void testGetSimpleCombinedProcessedTuple() throws Exception {
        CntMeanVar cmv = componentAggResult.getSimpleCombinedProcessedTuple();
        System.out.println(cmv);
    }
}