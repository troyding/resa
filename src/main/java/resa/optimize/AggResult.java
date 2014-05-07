package resa.optimize;

import java.util.Objects;

/**
 * Created by Tom.fu on 16/4/2014.
 */
public class AggResult implements Cloneable {

    protected QueueAggResult sendQueueResult = new QueueAggResult();
    protected QueueAggResult recvQueueResult = new QueueAggResult();

    public static <T extends AggResult> T getCombinedResult(T dest, Iterable<AggResult> his) {
        his.forEach(dest::add);
        return dest;
    }

    public void add(AggResult r) {
        Objects.requireNonNull(r, "input AggResult cannot null");
        this.sendQueueResult.add(r.sendQueueResult);
        this.recvQueueResult.add(r.recvQueueResult);
    }

    public QueueAggResult getSendQueueResult() {
        return sendQueueResult;
    }

    public QueueAggResult getRecvQueueResult() {
        return recvQueueResult;
    }
}
