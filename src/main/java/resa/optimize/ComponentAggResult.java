package resa.optimize;

import java.util.*;

/**
 * Created by Tom.fu on 16/4/2014.
 */
public class ComponentAggResult {

    CntMeanVar recvArrivalCnt = new CntMeanVar();
    CntMeanVar recvQueueLen = new CntMeanVar();
    CntMeanVar recvQueueSampleCnt = new CntMeanVar();

    CntMeanVar sendArrivalCnt = new CntMeanVar();
    CntMeanVar sendQueueLen = new CntMeanVar();
    CntMeanVar sendQueueSampleCnt = new CntMeanVar();

    Map<String, CntMeanVar> tupleProcess = new HashMap<>();

    MeasuredData.ComponentType type;

    ComponentAggResult(MeasuredData.ComponentType t) {
        this.type = t;
    }

    String getComponentType() {
        return type.name();
    }

    String getProcessString() {
        return type == MeasuredData.ComponentType.BOLT ? "exec-delay" : "complete-latency";
    }

    CntMeanVar getSimpleCombinedProcessedTuple() {
        CntMeanVar retVal = new CntMeanVar();
        tupleProcess.values().stream().forEach(retVal::addCMV);
        return retVal;
    }

    static void addCARto(ComponentAggResult from, ComponentAggResult to) {
        to.addCAR(from);
    }

    static ComponentAggResult getCombinedResult(Iterable<ComponentAggResult> his, MeasuredData.ComponentType t) {
        ComponentAggResult ret = new ComponentAggResult(t);
        his.forEach(ret::addCAR);
        return ret;
    }

    void addCAR(ComponentAggResult car) {
        if (this.type != Objects.requireNonNull(car).type) {
            throw new RuntimeException("Component mismatch");
        }

        this.recvArrivalCnt.addCMV(car.recvArrivalCnt);
        this.recvQueueLen.addCMV(car.recvQueueLen);
        this.recvQueueSampleCnt.addCMV(car.recvQueueSampleCnt);
        this.sendArrivalCnt.addCMV(car.sendArrivalCnt);
        this.sendQueueLen.addCMV(car.sendQueueLen);
        this.sendQueueSampleCnt.addCMV(car.sendQueueSampleCnt);

        car.tupleProcess.forEach((comp, cntMeanVar) -> {
            this.tupleProcess.computeIfAbsent(comp, (k) -> new CntMeanVar()).addCMV(cntMeanVar);
        });
    }
}
