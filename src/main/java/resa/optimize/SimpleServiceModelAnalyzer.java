package resa.optimize;

import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Created by Tom.fu on 23/4/2014.
 * Chain topology and not tuple split
 */
public class SimpleServiceModelAnalyzer {

    /**
     * Like module A in our discussion
     *
     * @param components,  the service node configuration, in this function, chain topology is assumed.
     * @param allocation,  can be null input, in this case, directly return Infinity to indicator topology unstable
     * @param printDetail, print calculation detail of each service node
     * @param showMinReq,  to show the minimun number of required server count for each service node for stable.
     * @return Double.MAX_VALUE when a) input allocation is null (i.e., system is unstable)
     * b) any one of the node is unstable (i.e., lambda/mu > 1, in which case, sn.estErlangT will be Double.MAX_VALUE)
     * else the validate estimated erlang service time.
     */
    public static double getErlangChainTopCompleteTime(
            Map<String, ServiceNode> components,
            Map<String, Integer> allocation,
            boolean printDetail, boolean showMinReq) {

        if (allocation == null) {
            return Double.MAX_VALUE;
        }
        double retVal = 0.0;

        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            String cid = e.getKey();
            ServiceNode sn = e.getValue();
            Integer serverCount = allocation.get(cid);
            Objects.requireNonNull(serverCount, "No allocation entry find for this component" + cid);

            double est = sn.estErlangT(serverCount);

            if (est < Double.MAX_VALUE) {
                retVal += est;
                if (printDetail) {
                    System.out.println(cid + String.format(", estT: %.5f, ", est) + sn.serviceNodeKeyStats(serverCount, showMinReq));
                }
            } else {
                return Double.MAX_VALUE;
            }
        }
        return retVal;
    }

    public static double getErlangChainTopCompleteTimeMilliSec(
            Map<String, ServiceNode> components,
            Map<String, Integer> allocation,
            boolean printDetail, boolean showMinReq) {
        double result = getErlangChainTopCompleteTime(components, allocation, printDetail, showMinReq);
        return result < Double.MAX_VALUE ? (result * 1000.0) : Double.MAX_VALUE;
    }

    public static double getErlangChainTopCompleteTime(Map<String, ServiceNode> components, Map<String, Integer> allocation) {
        return getErlangChainTopCompleteTime(components, allocation, false, false);
    }

    public static double getErlangChainTopCompleteTimeMilliSec(Map<String, ServiceNode> components, Map<String, Integer> allocation) {
        double result = getErlangChainTopCompleteTime(components, allocation);
        return result < Double.MAX_VALUE ? (result * 1000.0) : Double.MAX_VALUE;
    }

    public static Map<String, Integer> getAllocation(Map<String, ServiceNode> components, Map<String, Object> para) {
        Map<String, Integer> retVal = new HashMap<>();

        components.forEach((cid, sn) -> {
            int curr = ConfigUtil.getInt(para, cid, 0);
            retVal.put(cid, curr);
        });
        return retVal;
    }

    public static boolean checkStable(Map<String, ServiceNode> components, Map<String, Integer> allocation, boolean printDetail) {
        boolean ret = true;

        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            String cid = e.getKey();
            ServiceNode sn = e.getValue();
            Integer serverCount = allocation.get(cid);
            Objects.requireNonNull(serverCount, "No allocation entry find for this component" + cid);

            boolean s = sn.isStable(serverCount);
            if (printDetail) {
                System.out.println(cid + ", stable: " + s + ", " + sn.serviceNodeKeyStats(serverCount, false));
            }
            ret = ret & s;
        }
        return ret;
    }

    public static int getTotalMinRequirement(Map<String, ServiceNode> components) {
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            int minReq = e.getValue().getMinReqServerCount();
            if (minReq < Integer.MAX_VALUE) {
                totalMinReq += minReq;
            } else {
                return Integer.MAX_VALUE;
            }
        }
        return totalMinReq;
    }

    public static void printAllocation(Map<String, Integer> allocation) {
        if (allocation == null) {
            System.out.print("Null allocation input -> system is unstable.");
        } else {
            allocation.forEach((cid, serverCount) -> {
                System.out.print(" " + cid + ": " + serverCount);
            });
            System.out.println();
        }
    }

    /**
     * @param components
     * @param totalResourceCount
     * @param printDetail
     * @return, null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
     * b) total minReq can not be satisfied (total minReq > totalResourceCount)
     * otherwise, the Map data structure.
     */
    public static Map<String, Integer> suggestAllocation(Map<String, ServiceNode> components, int totalResourceCount, boolean printDetail) {
        Map<String, Integer> retVal = new HashMap<>();

        String defaultCID = null;

        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            String cid = e.getKey();
            defaultCID = cid;
            int minReq = e.getValue().getMinReqServerCount();

            retVal.put(cid, minReq);
            if (minReq == Integer.MAX_VALUE) {
                return null;
            }
            totalMinReq += minReq;
        }

        if (totalMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - totalMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = 0.0;
                String maxDiffCid = defaultCID;

                for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(cid);

                    double beforeAddT = sn.estErlangT(currentAllocated);
                    double afterAddT = sn.estErlangT(currentAllocated + 1);

                    double diff = beforeAddT - afterAddT;
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }

                int newAllocate = retVal.get(maxDiffCid) + 1;
                retVal.put(maxDiffCid, newAllocate);

                if (printDetail) {
                    System.out.println((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                }
            }
        } else {
            return null;
        }

        return retVal;
    }

    /**
     * Like Module A', input required QoS, output #threads required
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param components
     * @param maxAllowedCompleteTime, the unit here is second! consistent with function getErlangChainTopCompleteTime()
     * @param lowerBoundDelta,        this is to set the offset of the lowerBoundServiceTime, we require delta to be positive, and 0 as default.
     * @param adjRatio,               this is to adjust the estimated ErlangServiceTime to fit more closely to the real measured complte time
     * @return null if a) any service node is not in the valid state (mu = 0.0), this is not the case of rho > 1.0, just for checking mu
     * b) lowerBoundServiceTime > requiredQoS
     */
    public static Map<String, Integer> getMinReqServerAllocation(Map<String, ServiceNode> components,
                                                                 double maxAllowedCompleteTime,
                                                                 double lowerBoundDelta,
                                                                 double adjRatio) {
        double lowerBoundServiceTime = 0.0;
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : components.entrySet()) {
            double mu = e.getValue().getMu();
            if (mu == 0.0) {
                ///here we should check and throw an exception later on.
                throw new IllegalStateException("mu should be positive here");
            }
            ///caution, the unit should be millisecond
            lowerBoundServiceTime += (1.0 / mu);
            totalMinReq += e.getValue().getMinReqServerCount();
        }

        if (lowerBoundServiceTime + lowerBoundDelta > maxAllowedCompleteTime) {
            return null;
        }

        Map<String, Integer> currAllocation = suggestAllocation(components, totalMinReq, false);
        double currTime = getErlangChainTopCompleteTime(components, currAllocation) * adjRatio;

        while (currTime > maxAllowedCompleteTime) {
            totalMinReq++;
            currAllocation = suggestAllocation(components, totalMinReq, false);
            Objects.requireNonNull(currAllocation, "Allocation here should not be null!");
            currTime = getErlangChainTopCompleteTime(components, currAllocation) * adjRatio;
        }

        return currAllocation;
    }

    public static Map<String, Integer> getMinReqServerAllocation(Map<String, ServiceNode> components, double maxAllowedCompleteTime) {
        return getMinReqServerAllocation(components, maxAllowedCompleteTime, 0.0, 1.0);
    }

    public static Map<String, Integer> getMinReqServerAllocation(Map<String, ServiceNode> components, double maxAllowedCompleteTime, double adjRatio) {
        return getMinReqServerAllocation(components, maxAllowedCompleteTime, 0.0, adjRatio);
    }

    public static int totalServerCountInvolved(Map<String, Integer> allocation) {
        Objects.requireNonNull(allocation);
        int retVal = 0;
        for (Map.Entry<String, Integer> e : allocation.entrySet()) {
            retVal += e.getValue();
        }
        return retVal;
    }

    /**
     * Like module B in our discussion
     *
     * @param components
     * @param para
     * @param printDetail
     */
    public static void checkOptimized(Map<String, ServiceNode> components, Map<String, Object> para, boolean printDetail) {

        Map<String, Integer> curr = getAllocation(components, para);

        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        double estimatedLatencyMilliSec = getErlangChainTopCompleteTimeMilliSec(components, curr);
        double realLatencyMilliSec = ConfigUtil.getDouble(para, "avgCompleteHisMilliSec", estimatedLatencyMilliSec);

        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSec / estimatedLatencyMilliSec);

        double targetQoSMilliSec = ConfigUtil.getDouble(para, "QoS", 5000.0);
        boolean targetQoSSatisfied = estimatedLatencyMilliSec < targetQoSMilliSec;
        int currAllocationCount = totalServerCountInvolved(curr);

        System.out.println("estimated: " + estimatedLatencyMilliSec + ", estiQoSSatisfied: " + targetQoSSatisfied
                + ", real: " + realLatencyMilliSec + ", realQoSSatisfied: " + (realLatencyMilliSec < targetQoSMilliSec));

        Map<String, Integer> minReqAllocation = getMinReqServerAllocation(components, targetQoSMilliSec / 1000.0, underEstimateRatio);
        int minReqTotalServerCount = minReqAllocation == null ? Integer.MAX_VALUE : totalServerCountInvolved(minReqAllocation);
        double minReqQoSMilliSec = getErlangChainTopCompleteTimeMilliSec(components, minReqAllocation);
        double adjMinReqQoSMilliSec = getErlangChainTopCompleteTimeMilliSec(components, minReqAllocation) * underEstimateRatio;

        if (minReqAllocation == null) {
            System.out.println("Caution: Target QoS is problematic, can not be achieved!");
        } else {
            System.out.println("MinReqTotalServerCount: " + minReqTotalServerCount + ", minReqQoS: " + minReqQoSMilliSec);
            System.out.println("underEstimateRatio: " + underEstimateRatio + ", adjMinReqQoS: " + adjMinReqQoSMilliSec + ", optAllo: ");
            printAllocation(minReqAllocation);
        }

        if (minReqAllocation != null) {
            int remainCount = minReqTotalServerCount - currAllocationCount;
            if (remainCount > 0) {
                System.out.println("Require " + remainCount + " additional threads!!!");
            } else {
                System.out.println("Rebalance the current to suggested");
                Map<String, Integer> after = suggestAllocation(components, currAllocationCount, printDetail);
                System.out.println("---------------------- Current Allocation ----------------------");
                printAllocation(curr);
                System.out.println("---------------------- Suggested Allocation ----------------------");
                printAllocation(after);
            }
        } else {
            System.out.println("Caution: Target QoS can never be achieved!");
        }

    }
}
