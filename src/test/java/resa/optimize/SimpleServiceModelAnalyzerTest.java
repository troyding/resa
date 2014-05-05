package resa.optimize;

import org.junit.Test;
import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tom.fu on 2/5/2014.
 */
public class SimpleServiceModelAnalyzerTest {
    @Test
    public void testGetErlangChainTopCompleteTime() throws Exception {

        Map<String, ServiceNode> components = new HashMap<>();
        Map<String, Object> para = new HashMap<>();

        components.put("split", new ServiceNode(8.0, 9.639, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(8.008, 4.855, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        para.put("split", 4);
        para.put("counter", 2);

        Map<String, Integer> allo = SimpleServiceModelAnalyzer.getAllocation(components, para);
        double ret = SimpleServiceModelAnalyzer.getErlangChainTopCompleteTime(components, allo);

        System.out.println(ret);
    }

    @Test
    public void testGetErlangChainTopCompleteTimeMilliSec() throws Exception {
        Map<String, ServiceNode> components = new HashMap<>();
        Map<String, Object> para = new HashMap<>();

        components.put("split", new ServiceNode(8.0, 9.639, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(8.008, 4.855, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        para.put("split", 4);
        para.put("counter", 2);

        Map<String, Integer> allo = SimpleServiceModelAnalyzer.getAllocation(components, para);
        double ret = SimpleServiceModelAnalyzer.getErlangChainTopCompleteTimeMilliSec(components, allo);

        System.out.println(ret);
    }

    @Test
    public void testGetAllocation() throws Exception {

        Map<String, ServiceNode> components = new HashMap<>();
        Map<String, Object> para = new HashMap<>();

        components.put("split", new ServiceNode(2.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(2.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("bolt3", new ServiceNode(2.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        para.put("split", 1);
        para.put("counter", 2);
        para.put("bolt3", 3);

        Map<String, Integer> allo = SimpleServiceModelAnalyzer.getAllocation(components, para);

        System.out.println(allo);

    }

    @Test
    public void testCheckStable() throws Exception {
        Map<String, ServiceNode> components = new HashMap<>();
        Map<String, Object> para = new HashMap<>();

        components.put("split", new ServiceNode(2.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(6.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("bolt3", new ServiceNode(7.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        para.put("split", 3);
        para.put("counter", 2);
        para.put("bolt3", 1);

        Map<String, Integer> allo = SimpleServiceModelAnalyzer.getAllocation(components, para);

        boolean ret = SimpleServiceModelAnalyzer.checkStable(components, allo);
        System.out.println(allo);
        System.out.println(ret);
    }

    @Test
    public void testGetTotalMinRequirement() throws Exception {
        Map<String, ServiceNode> components = new HashMap<>();
        Map<String, Object> para = new HashMap<>();

        components.put("split", new ServiceNode(2.0, 1.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(6.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("bolt3", new ServiceNode(2.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        int ret =SimpleServiceModelAnalyzer.getTotalMinRequirement(components);
        System.out.println(ret);
    }

    @Test
    public void testSuggestAllocation() throws Exception {
        Map<String, ServiceNode> components = new HashMap<>();

        components.put("split", new ServiceNode(11.0, 10.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(11.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        Map<String, Integer> allo = SimpleServiceModelAnalyzer.suggestAllocation(components, 6);
        System.out.println(allo);
    }

    @Test
    public void testGetMinReqServerAllocation() throws Exception {
        Map<String, ServiceNode> components = new HashMap<>();
        Map<String, Object> para = new HashMap<>();

        components.put("split", new ServiceNode(8.0, 9.639, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(8.008, 4.855, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        para.put("split", 4);
        para.put("counter", 2);

        Map<String, Integer> allo = SimpleServiceModelAnalyzer.getMinReqServerAllocation(components, 1500, 0,  1.0234548286107188);
        System.out.println(allo);
    }

    @Test
    public void testTotalServerCountInvolved() throws Exception {

        Map<String, ServiceNode> components = new HashMap<>();
        Map<String, Object> para = new HashMap<>();

        components.put("split", new ServiceNode(2.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(6.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("bolt3", new ServiceNode(7.0, 5.0, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        para.put("split", 3);
        para.put("counter", 2);
        para.put("bolt3", 1);

        Map<String, Integer> allo = SimpleServiceModelAnalyzer.getAllocation(components, para);

        int ret = SimpleServiceModelAnalyzer.totalServerCountInvolved(allo);
        System.out.println(allo);
        System.out.println(ret);


    }

    @Test
    public void testCheckOptimized() throws Exception {

        Map<String, ServiceNode> components = new HashMap<>();
        components.put("split", new ServiceNode(8.0, 9.639, ServiceNode.ServiceType.EXPONENTIAL, 1.0));
        components.put("counter", new ServiceNode(8.008, 4.855, ServiceNode.ServiceType.EXPONENTIAL, 1.0));

        Map<String, Object> conf = new HashMap<>();
        conf.put("avgCompleteHisMilliSec", 765.9786516853933);
        conf.put("QoS", 1500);

        Map<String, Integer> currBoltAllocation = new HashMap<>();
        currBoltAllocation.put("split", 4);
        currBoltAllocation.put("counter", 2);

        int maxAvailable4Bolt = 6;

        OptimizeDecision ret = SimpleServiceModelAnalyzer.checkOptimized(components, 765.9786516853933, 1500,  currBoltAllocation, maxAvailable4Bolt);

        double estimatedLatencyMilliSec = SimpleServiceModelAnalyzer.getErlangChainTopCompleteTimeMilliSec(components, currBoltAllocation);
        double realLatencyMilliSec = ConfigUtil.getDouble(conf, "avgCompleteHisMilliSec", estimatedLatencyMilliSec);
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSec / estimatedLatencyMilliSec);

        double targetQoSMilliSec = ConfigUtil.getDouble(conf, "QoS", 5000.0);
        boolean targetQoSSatisfied = estimatedLatencyMilliSec < targetQoSMilliSec;
        int currAllocationCount = SimpleServiceModelAnalyzer.totalServerCountInvolved(currBoltAllocation);

        System.out.println("estimated: " + estimatedLatencyMilliSec + ", estiQoSSatisfied: " + targetQoSSatisfied + ", real: "
                + realLatencyMilliSec + ", realQoSSatisfied: " + (realLatencyMilliSec < targetQoSMilliSec));

        Map<String, Integer> minReqAllocation = SimpleServiceModelAnalyzer.getMinReqServerAllocation(components, targetQoSMilliSec / 1000.0,
                underEstimateRatio);
        int minReqTotalServerCount = minReqAllocation == null ? Integer.MAX_VALUE :
                SimpleServiceModelAnalyzer.totalServerCountInvolved(minReqAllocation);
        double minReqQoSMilliSec = SimpleServiceModelAnalyzer.getErlangChainTopCompleteTimeMilliSec(components,
                minReqAllocation);
        double adjMinReqQoSMilliSec = SimpleServiceModelAnalyzer.getErlangChainTopCompleteTimeMilliSec(components, minReqAllocation) *
                underEstimateRatio;

        System.out.println(currBoltAllocation);
        System.out.println(ret.currOptAllocation);
        System.out.println(ret.minReqOptAllocation);
        System.out.println(ret.status);

        if (minReqAllocation == null) {
            System.out.println("Caution: Target QoS is problematic, can not be achieved!");
        } else {
            System.out.println("MinReqTotalServerCount: " + minReqTotalServerCount + ", minReqQoS: " + minReqQoSMilliSec);
            System.out.println("underEstimateRatio: " + underEstimateRatio + ", adjMinReqQoS: " + adjMinReqQoSMilliSec
                    + ", optAllo: ");
            SimpleServiceModelAnalyzer.printAllocation(minReqAllocation);
        }

        if (minReqAllocation != null) {
            int remainCount = minReqTotalServerCount - currAllocationCount;
            if (remainCount > 0) {
                System.out.println("Require " + remainCount + " additional threads!!!");
            } else {
                System.out.println("Rebalance the current to suggested");
                Map<String, Integer> after = SimpleServiceModelAnalyzer.suggestAllocation(components, currAllocationCount);
                System.out.println("---------------------- Current Allocation ----------------------");
                SimpleServiceModelAnalyzer.printAllocation(currBoltAllocation);
                System.out.println("---------------------- Suggested Allocation ----------------------");
                SimpleServiceModelAnalyzer.printAllocation(after);
            }
        } else {
            System.out.println("Caution: Target QoS can never be achieved!");
        }
    }
}
