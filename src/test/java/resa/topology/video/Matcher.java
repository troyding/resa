package resa.topology.video;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import resa.util.ConfigUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static resa.topology.video.Constant.*;

/**
 * Created by ding on 14-7-3.
 */
public class Matcher extends BaseRichBolt {

    private static final int[] EMPTY_MATCH = new int[0];
    private Map<float[], int[]> featDesc2Image;
    private OutputCollector collector;
    private double distThreshold;

    @Override

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        featDesc2Image = new HashMap<>();
        loadIndex(context.getThisTaskIndex(), context.getComponentTasks(context.getThisComponentId()).size());
        this.collector = collector;
        distThreshold = ConfigUtil.getDouble(stormConf, CONF_FEAT_DIST_THRESHOLD, 100);
    }

    private void loadIndex(int index, int totalPieces) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream("/index.txt")))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty() || count++ % totalPieces == index) {
                    continue;
                }
                StringTokenizer tokenizer = new StringTokenizer(line);
                String[] tmp = StringUtils.split(tokenizer.nextToken(), ',');
                float[] feat = new float[tmp.length];
                for (int i = 0; i < feat.length; i++) {
                    feat[i] = Float.parseFloat(tmp[i]);
                }
                int[] images = Stream.of(StringUtils.split(tokenizer.nextToken(), ',')).mapToInt(Integer::parseInt)
                        .toArray();
                featDesc2Image.put(feat, images);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String frameId = input.getStringByField(FIELD_FRAME_ID);
        List<float[]> desc = (List<float[]>) input.getValueByField(FIELD_FEATURE_DESC);
        Map<Integer, Long> image2Freq = desc.stream().map(this::findMatches)
                .flatMap(imgList -> IntStream.of(imgList).boxed())
                .collect(Collectors.groupingBy(i -> i, Collectors.counting()));
        int[] matches = image2Freq.isEmpty() ? EMPTY_MATCH : new int[image2Freq.size() * 2];
        int i = 0;
        for (Map.Entry<Integer, Long> m : image2Freq.entrySet()) {
            matches[i++] = m.getKey();
            matches[i++] = m.getValue().intValue();
        }
        collector.emit(STREAM_MATCH_IMAGES, input, new Values(frameId, matches));
        collector.ack(input);
    }

    private int[] findMatches(float[] desc) {
        double dist = Double.MAX_VALUE;
        int[] matches = EMPTY_MATCH;
        for (Map.Entry<float[], int[]> e : featDesc2Image.entrySet()) {
            double d = distance(e.getKey(), desc);
            if (d < dist) {
                dist = d;
                matches = e.getValue();
            }
        }
        if (dist > distThreshold) {
            matches = EMPTY_MATCH;
        }
        return matches;
    }

    private double distance(float[] v1, float[] v2) {
        double sum = 0;
        for (int i = 0; i < v1.length; i++) {
            double d = v1[i] - v2[1];
            sum += d * d;
        }
        return Math.sqrt(sum);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_MATCH_IMAGES, new Fields(FIELD_FRAME_ID, FIELD_MATCH_IMAGES));
    }

}
