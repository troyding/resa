package resa.util;

/**
 * Created by ding on 14-4-26.
 */
public class Sampler {

    private int sampleValue;
    private long counter = 0;

    public Sampler(double rate) {
        sampleValue = (int) (Math.random() / rate);
    }

    public boolean shoudSample() {
        return counter++ % sampleValue == 0;
    }

}
