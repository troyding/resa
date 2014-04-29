package resa.optimize;

import java.util.Map;

/**
 * Created by ding on 14-4-28.
 */
public class MeasuredData {

    public static enum ComponentType {BOLT, SPOUT}

    public final long timestamp;
    public final String component;
    public final int task;
    public final Map<String, Object> data;
    public final ComponentType componentType;

    public MeasuredData(ComponentType type, String component, int task, long timestamp, Map<String, Object> data) {
        this.componentType = type;
        this.timestamp = timestamp;
        this.component = component;
        this.task = task;
        this.data = data;
    }
}
