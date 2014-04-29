package resa.optimize;

import org.junit.Test;

public class ComponentAggResultTest {

    private ComponentAggResult componentAggResult = new ComponentAggResult(ComponentAggResult.ComponentType.bolt);

    @Test
    public void testGetSimpleCombinedProcessedTuple() throws Exception {
        CntMeanVar cmv = componentAggResult.getSimpleCombinedProcessedTuple();
        System.out.println(cmv);
    }
}