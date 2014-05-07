package resa.optimize;

/**
 * Created by ding on 14-5-6.
 */
public class QueueAggResult implements Cloneable {

    private long arrivalCount;
    private long duration;
    private long totalQueueLenth;
    private int totalSampleCount;

    public QueueAggResult(long arrivalCount, long duration, long totalQueueLenth, int totalSampleCount) {
        this.arrivalCount = arrivalCount;
        this.duration = duration;
        this.totalQueueLenth = totalQueueLenth;
        this.totalSampleCount = totalSampleCount;
    }

    public QueueAggResult() {
        this(0, 0, 0, 0);
    }

    public int getAvgQueueLength() {
        return (int) (totalQueueLenth / totalSampleCount);
    }

    public double getArrivalRatePerSec() {
        return arrivalCount * 1000.0 / (double) duration;
    }

    public long getArrivalCount() {
        return arrivalCount;
    }

    public long getTotalQueueLenth() {
        return totalQueueLenth;
    }

    public long getDuration() {
        return duration;
    }

    public int getTotalSampleCount() {
        return totalSampleCount;
    }

    public void add(QueueAggResult result) {
        arrivalCount += result.arrivalCount;
        duration += result.duration;
        totalQueueLenth += result.totalQueueLenth;
        totalSampleCount += result.totalSampleCount;
    }

    public void add(long arrivalCount, long duration, long totalQueueLenth, int totalSampleCount) {
        this.arrivalCount += arrivalCount;
        this.duration += duration;
        this.totalQueueLenth += totalQueueLenth;
        this.totalSampleCount += totalSampleCount;
    }
}
