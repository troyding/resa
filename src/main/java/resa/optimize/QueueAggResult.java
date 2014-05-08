package resa.optimize;

/**
 * Created by ding on 14-5-6.
 */
public class QueueAggResult implements Cloneable {

    private long arrivalCount;
    private long duration;
    private long totalQueueLength;
    private int totalSampleCount;

    public QueueAggResult(long arrivalCount, long duration, long totalQueueLength, int totalSampleCount) {
        this.arrivalCount = arrivalCount;
        this.duration = duration;
        this.totalQueueLength = totalQueueLength;
        this.totalSampleCount = totalSampleCount;
    }

    public QueueAggResult() {
        this(0, 0, 0, 0);
    }

    public double getAvgQueueLength() {
        return totalSampleCount > 0 ? (double)totalQueueLength / (double)totalSampleCount : 0.0;
    }

    public double getArrivalRatePerSec() {
        return arrivalCount * 1000.0 / (double) duration;
    }

    public long getArrivalCount() {
        return arrivalCount;
    }

    public long getTotalQueueLength() {
        return totalQueueLength;
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
        totalQueueLength += result.totalQueueLength;
        totalSampleCount += result.totalSampleCount;
    }

    public void add(long arrivalCount, long duration, long totalQueueLength, int totalSampleCount) {
        this.arrivalCount += arrivalCount;
        this.duration += duration;
        this.totalQueueLength += totalQueueLength;
        this.totalSampleCount += totalSampleCount;
    }

    @Override
    public String toString() {
        return String.format("arrivalCount: %d, duration: %d, totalQLength: %d, totalSamCount: %d, arrivalRatePerSec: %.5f", arrivalCount, duration, totalQueueLength, totalSampleCount, getArrivalRatePerSec());
    }
}
