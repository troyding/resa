package resa.optimize;

import resa.metrics.MetricNames;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ding on 14-3-4.
 * Note:
 * Recv-Queue arrival count includes ack for each message
 * When calculate sum and average, need to adjust (sum - #message, average - 1) for accurate value.
 */
public class AggResultCalculator {

    public AggResultCalculator(Iterable<MeasuredData> dataStream) {
        this.dataStream = dataStream;
    }

    protected Iterable<MeasuredData> dataStream;

    private Map<Integer, ComponentAggResult> spoutResult = new HashMap<>();
    private Map<Integer, ComponentAggResult> boltResult = new HashMap<>();

    public void calCMVStat() {

        for (MeasuredData measuredData : dataStream) {
            ///Real example
            //69) "objectSpout:4->{\"receive\":{\"sampleCount\":209,\"totalQueueLen\":212,\"totalCount\":4170},\"complete-latency\":{\"default\":\"2086,60635.0,3382707.0\"},\"sendqueue\":{\"sampleCount\":420,\"totalQueueLen\":424,\"totalCount\":8402}}"
            //70) "projection:7->{\"receive\":{\"sampleCount\":52,\"totalQueueLen\":53,\"totalCount\":1052},\"sendqueue\":{\"sampleCount\":2152,\"totalQueueLen\":4514,\"totalCount\":43052},\"execute\":{\"objectSpout:default\":\"525,709.4337659999997,1120.8007487084597\"}}"
            //71) "detector:3->{\"receive\":{\"sampleCount\":2769,\"totalQueueLen\":6088758,\"totalCount\":55416},\"sendqueue\":{\"sampleCount\":8921,\"totalQueueLen\":11476,\"totalCount\":178402},\"execute\":{\"projection:default\":\"49200,5167.623237000047,721.6383647758853\"}}"
            //73) "updater:9->{\"receive\":{\"sampleCount\":3921,\"totalQueueLen\":5495,\"totalCount\":78436},\"sendqueue\":{\"sampleCount\":4001,\"totalQueueLen\":4336,\"totalCount\":80002},\"execute\":{\"detector:default\":\"40000,1651.7782049999894,182.68124734051045\"}}"
            Map<String, Object> componentData = measuredData.data;

            String componentName = measuredData.component;
            int taskId = measuredData.task;
            ComponentAggResult car;
            if (measuredData.componentType.equals(MeasuredData.ComponentType.SPOUT)) {
                car = spoutResult.computeIfAbsent(taskId, (k) -> new ComponentAggResult(MeasuredData.ComponentType.SPOUT));
            } else {
                car = boltResult.computeIfAbsent(taskId, (k) -> new ComponentAggResult(MeasuredData.ComponentType.BOLT));
            }

            for (Map.Entry<String, Object> e : componentData.entrySet()) {
                switch (e.getKey()) {
                    case MetricNames.COMPLETE_LATENCY:
                    case MetricNames.TASK_EXECUTE:
                        ((Map<String, Object>) e.getValue()).forEach((streamName, elementStr) -> {
                            String[] elements = ((String) elementStr).split(",");
                            int cnt = Integer.valueOf(elements[0]);
                            if (cnt > 0) {
                                double val = Double.valueOf(elements[1]);
                                double val_2 = Double.valueOf(elements[2]);
                                car.tupleProcess.computeIfAbsent(streamName, (k) -> new CntMeanVar())
                                        .addAggWin(cnt, val, val_2);
                            }
                        });
                        break;
                    case MetricNames.SEND_QUEUE: {
                        Map<String, Number> queueMetrics = (Map<String, Number>) e.getValue();
                        long sampleCnt = queueMetrics.getOrDefault("sampleCount", Integer.valueOf(0)).longValue();
                        long totalQLen = queueMetrics.getOrDefault("totalQueueLen", Integer.valueOf(0)).longValue();
                        long totalArrivalCnt = queueMetrics.getOrDefault("totalCount", Integer.valueOf(0)).longValue();

                        if (sampleCnt > 0) {
                            car.sendQueueSampleCnt.addOneNumber(sampleCnt);
                            double avgQLen = (double) totalQLen / sampleCnt;
                            car.sendQueueLen.addOneNumber(avgQLen);
                        }

                        if (totalArrivalCnt > 0) {
                            car.sendArrivalCnt.addOneNumber(totalArrivalCnt);
                        }
                        break;
                    }
                    case MetricNames.RECV_QUEUE: {
                        Map<String, Number> queueMetrics = (Map<String, Number>) e.getValue();
                        long sampleCnt = queueMetrics.getOrDefault("sampleCount", Integer.valueOf(0)).longValue();
                        long totalQLen = queueMetrics.getOrDefault("totalQueueLen", Integer.valueOf(0)).longValue();
                        long totalArrivalCnt = queueMetrics.getOrDefault("totalCount", Integer.valueOf(0)).longValue();

                        if (sampleCnt > 0) {
                            car.recvQueueSampleCnt.addOneNumber((double) sampleCnt);
                            double avgQLen = (double) totalQLen / (double) sampleCnt;
                            car.recvQueueLen.addOneNumber(avgQLen);
                        }
                        if (totalArrivalCnt > 0) {
                            car.recvArrivalCnt.addOneNumber((double) totalArrivalCnt);
                        }
                        break;
                    }
                    default:
                        throw new IllegalStateException("Cannot reach here");
                }
            }
        }
    }

    public Map<Integer, ComponentAggResult> getSpoutResult() {
        return spoutResult;
    }

    public Map<Integer, ComponentAggResult> getBoltResult() {
        return boltResult;
    }

    public static void printCMVStat(Map<String, ComponentAggResult> result) {
        if (result == null) {
            System.out.println("input AggResult is null.");
            return;
        }

        for (Map.Entry<String, ComponentAggResult> e : result.entrySet()) {
            String cid = e.getKey();
            String componentName = cid.split(":")[0];
            String taskID = cid.split(":")[1];

            ComponentAggResult car = e.getValue();
            int tupleProcessCnt = car.tupleProcess.size();

            System.out.println("-------------------------------------------------------------------------------");
            System.out.println("ComponentName: " + componentName + ", taskID: " + taskID + ", type: " + car.getComponentType() + ", tupleProcessCnt: " + tupleProcessCnt);
            ///System.out.println("---------------------------------------------------------------------------");
            System.out.println("SendQueue->SampleCnt: " + car.sendQueueSampleCnt.toCMVString());
            System.out.println("SendQueue->QueueLen: " + car.sendQueueLen.toCMVString());
            System.out.println("SendQueue->Arrival: " + car.sendArrivalCnt.toCMVString());
            ///System.out.println("---------------------------------------------------------------------------");
            System.out.println("RecvQueue->SampleCnt: " + car.recvQueueSampleCnt.toCMVString());
            System.out.println("RecvQueue->QueueLen: " + car.recvQueueLen.toCMVString());
            System.out.println("RecvQueue->Arrival: " + car.recvArrivalCnt.toCMVString());
            ///System.out.println("---------------------------------------------------------------------------");
            if (tupleProcessCnt > 0) {
                for (Map.Entry<String, CntMeanVar> innerE : car.tupleProcess.entrySet()) {
                    System.out.println(car.getProcessString() + "->" + innerE.getKey() + ":" + innerE.getValue().toCMVString());
                }
            }
            System.out.println("-------------------------------------------------------------------------------");
        }

    }

    public static void printCMVStatShort(Map<String, ComponentAggResult> result) {
        if (result == null) {
            System.out.println("input AggResult is null.");
            return;
        }

        for (Map.Entry<String, ComponentAggResult> e : result.entrySet()) {
            String cid = e.getKey();
            String componentName = cid.split(":")[0];
            String taskID = cid.split(":")[1];

            ComponentAggResult car = e.getValue();
            int tupleProcessCnt = car.tupleProcess.size();

            System.out.print(componentName + ":" + taskID + ":" + car.getComponentType());
            System.out.print(",RQ:" + car.recvQueueLen.toCMVStringShort());
            System.out.print(",Arrl:" + car.recvArrivalCnt.toCMVStringShort());
            System.out.println(",SQ:" + car.sendQueueLen.toCMVStringShort());

            if (tupleProcessCnt > 0) {
                for (Map.Entry<String, CntMeanVar> innerE : car.tupleProcess.entrySet()) {
                    System.out.println(car.getProcessString() + "->" + innerE.getKey() + ":" + innerE.getValue().toCMVString());
                }
            }
            System.out.println("-------------------------------------------------------------------------------");
        }
    }
}
