package resa.optimize;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import clojure.lang.MapEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import resa.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by ding on 14-4-30.
 */
public class SimpleGeneralDecisionMaker extends DecisionMaker {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleGeneralDecisionMaker.class);
    private AggregatedData spoutAregatedData;
    private AggregatedData boltAregatedData;
    private int historySize;
    private int currHistory;

    @Override
    public void init(Map<String, Object> conf, Map<String, Integer> currAllocation, StormTopology rawTopology) {
        super.init(conf, currAllocation, rawTopology);
        historySize = ConfigUtil.getInt(conf, "resa.opt.win.history.size", 1);
        currHistory = 0;
        spoutAregatedData = new AggregatedData(rawTopology, historySize);
        boltAregatedData = new AggregatedData(rawTopology, historySize);
    }

    @Override
    public Map<String, Integer> make(Map<String, AggResult[]> executorAggResults, int maxAvailableExecutors) {
        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .forEach(e -> spoutAregatedData.putResult(e.getKey(), e.getValue()));
        executorAggResults.entrySet().stream().filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .forEach(e -> boltAregatedData.putResult(e.getKey(), e.getValue()));
        // check history size. Ensure we have enough history data before we run the optimize function
        currHistory++;
        if (currHistory < historySize) {
            LOG.info("currHistory < historySize, curr: " + currHistory + ", Size: " + historySize);
            return null;
        } else {
            currHistory = historySize;
        }

        ///Temp use, assume only one running topology!
        double targetQoSMs = ConfigUtil.getDouble(conf, "resa.opt.smd.qos.ms", 5000.0);
        int maxSendQSize = ConfigUtil.getInt(conf, Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 1024);
        int maxRecvQSize = ConfigUtil.getInt(conf, Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
        double sendQSizeThresh = ConfigUtil.getDouble(conf, "resa.opt.smd.sq.thresh", 5.0);
        double recvQSizeThreshRatio = ConfigUtil.getDouble(conf, "resa.opt.smd.rq.thresh.ratio", 0.6);
        double recvQSizeThresh = recvQSizeThreshRatio * maxRecvQSize;

        double componentSampelRate = ConfigUtil.getDouble(conf, "resa.comp.sample.rate", 1.0);
        /*
        double avgCompleteHis = spoutAregatedData.compHistoryResults.entrySet().stream().mapToDouble(e -> {
            Iterable<AggResult> results = e.getValue();
            SpoutAggResult hisCar = AggResult.getCombinedResult(new SpoutAggResult(), results);
            CntMeanVar hisCarCombined = hisCar.getCombinedCompletedLatency();
            return hisCarCombined.getAvg();
        }).average().getAsDouble();
        */
        ///Here we assume only one spout

        Map<String, SourceNode> spInfos = spoutAregatedData.compHistoryResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    Iterable<AggResult> results = e.getValue();
                    SpoutAggResult hisCar = AggResult.getCombinedResult(new SpoutAggResult(), results);
                    CntMeanVar hisCarCombined = hisCar.getCombinedCompletedLatency();

                    double avgSendQLenHis = hisCar.getSendQueueResult().getAvgQueueLength();
                    double avgRecvQLenHis = hisCar.getRecvQueueResult().getAvgQueueLength();
                    double arrivalRateHis = hisCar.getRecvQueueResult().getArrivalRatePerSec();
                    double departRateHis = hisCar.getSendQueueResult().getArrivalRatePerSec();

                    double avgCompleteHis = hisCarCombined.getAvg();///unit is millisecond

                    double totalComplteTupleCnt = hisCarCombined.getCount();
                    double totalDuration = hisCar.getDuration();

                    ///TODO: here are some problem not solved yet. calculation is incorrect.
                    double tupleCompleteRate
                            = totalComplteTupleCnt * 1000.0 / (totalDuration * componentSampelRate);

                    int numberExecutor = currAllocation.get(e.getKey());
                    ///TODO: there we multiply 1/2 for this particular implementation
                    double tupleEmitRate = departRateHis * numberExecutor / 2.0;

                    LOG.info("exec(name, count): (" + e.getKey() + "," + numberExecutor
                            + "), tFinCnt: " + totalComplteTupleCnt + ", sumDur: " + totalDuration
                            + ", tFinRate: " + tupleCompleteRate);
                    LOG.info("avgSQLenHis: " + avgSendQLenHis + ",avgRQLenHis: " + avgRecvQLenHis
                            + ", RQarrRateHis: " + arrivalRateHis + ", SQarrRateHis: " + departRateHis);
                    LOG.info("avgCompleHis: " + avgCompleteHis + ", tupleEmitRate: " + tupleEmitRate);

                return new SourceNode(avgCompleteHis, totalComplteTupleCnt, totalDuration, tupleEmitRate);
                }));

        SourceNode spInfo = spInfos.entrySet().stream().findFirst().get().getValue();

        Map<String, ServiceNode> queueingNetwork = boltAregatedData.compHistoryResults.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    Iterable<AggResult> results = e.getValue();
                    BoltAggResult hisCar = AggResult.getCombinedResult(new BoltAggResult(), results);
                    CntMeanVar hisCarCombined = hisCar.getCombinedProcessedResult();

                    double avgSendQLenHis = hisCar.getSendQueueResult().getAvgQueueLength();
                    double avgRecvQLenHis = hisCar.getRecvQueueResult().getAvgQueueLength();
                    double arrivalRateHis = hisCar.getArrivalRatePerSec();
                    double avgServTimeHis = hisCarCombined.getAvg();///unit is millisecond

                    double lambdaHis = arrivalRateHis * currAllocation.get(e.getKey());
                    double muHis = 1000.0 / avgServTimeHis;
                    //TODO: when processed tuple count is very small (e.g. there is no input tuple),
                    // avgServTime becomes zero and mu becomes infinity, this will cause problematic SN.
                    double rhoHis = lambdaHis / muHis;

                    boolean sendQLenNormalHis = avgSendQLenHis < sendQSizeThresh;
                    boolean recvQlenNormalHis = avgRecvQLenHis < recvQSizeThresh;

                    double i2oRatio = lambdaHis / spInfo.getTupleLeaveRateOnSQ();
                    int numberExecutor = currAllocation.get(e.getKey());
                    LOG.info("exec(name, count): (" + e.getKey() + "," + numberExecutor + ")");
                    LOG.info("avgSQLenHis: " + avgSendQLenHis + ",avgRQLenHis: " + avgRecvQLenHis + ", arrRateHis: "
                            + arrivalRateHis + ", avgServTimeHis(ms): " + avgServTimeHis);
                    LOG.info("rhoHis: " + rhoHis + ", lambdaHis: " + lambdaHis + ", muHis: " + muHis + ", ratio: " + i2oRatio);

                    return new ServiceNode(lambdaHis, muHis, ServiceNode.ServiceType.EXPONENTIAL, i2oRatio);
                }));
        int maxThreadAvailable4Bolt = maxAvailableExecutors - currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_spouts().containsKey(e.getKey()))
                .mapToInt(Map.Entry::getValue).sum();
        Map<String, Integer> boltAllocation = currAllocation.entrySet().stream()
                .filter(e -> rawTopology.get_bolts().containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        OptimizeDecision optimizeDecision = SimpleGeneralServiceModel.checkOptimized(queueingNetwork,
                spInfo.getRealLatencyMilliSec(), targetQoSMs, boltAllocation, maxThreadAvailable4Bolt);
        LOG.debug("minReq: {} " + optimizeDecision.minReqOptAllocation + ", status: " + optimizeDecision.status);
        Map<String, Integer> ret = new HashMap<>(currAllocation);
        // merge the optimized decision into source allocation
        ret.putAll(optimizeDecision.currOptAllocation);
        return ret;
    }

    @Override
    public void allocationChanged(Map<String, Integer> newAllocation) {
        super.allocationChanged(newAllocation);
        spoutAregatedData.clear();
        boltAregatedData.clear();
        currHistory = 0;
    }
}
