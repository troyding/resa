package resa.optimize;

import java.util.Collections;
import java.util.Map;

/**
 * Created by ding on 14-4-29.
 */
public class OptimizeDecision {

    //TODO: add expected QoS for both minReqOptAllocation and currOptAllocation
    //so that for later programme to optimize the rebalance behavior
    // (e.g. consider expected rebalance gain vs. cost)

    public static enum Status {
        INFEASIBLE, FEASIBALE
    }

    public final Status status;
    public final Map<String, Integer> minReqOptAllocation;
    public final Map<String, Integer> currOptAllocation;

    public OptimizeDecision(Status status, Map<String, Integer> minReqOptAllocation,
                            Map<String, Integer> currOptAllocation) {
        this.status = status;
        this.minReqOptAllocation = minReqOptAllocation;
        this.currOptAllocation = currOptAllocation;
    }

    public OptimizeDecision(Status status, Map<String, Integer> currOptAllocation) {
        this(status, Collections.EMPTY_MAP, currOptAllocation);
    }
}
