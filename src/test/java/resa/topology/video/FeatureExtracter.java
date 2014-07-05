package resa.topology.video;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacpp.opencv_features2d.KeyPoint;
import org.bytedeco.javacpp.opencv_nonfree.SIFT;
import resa.util.ConfigUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.cvDecodeImage;
import static resa.topology.video.Constant.*;

/**
 * Created by ding on 14-7-3.
 */
public class FeatureExtracter extends BaseRichBolt {

    private SIFT sift;
    private double[] buf;
    private OutputCollector collector;
    private double prefiterDist;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        sift = new SIFT();
        buf = new double[128];
        this.collector = collector;
        prefiterDist = ConfigUtil.getDouble(stormConf, CONF_FEAT_PREFILTER_THRESHOLD, Double.MAX_VALUE);
    }

    @Override
    public void execute(Tuple input) {
        byte[] imgBytes = (byte[]) input.getValueByField(FIELD_IMG_BYTES);
        IplImage image = cvDecodeImage(cvMat(1, imgBytes.length, CV_8UC1, new BytePointer(imgBytes)));
        KeyPoint points = new KeyPoint();
        Mat featureDesc = new Mat();
        try {
            Mat matImg = new Mat(image);
            sift.detect(matImg, points);
            sift.compute(matImg, points, featureDesc);
        } finally {
            cvReleaseImage(image);
        }
        List<float[]> selected = new ArrayList<>();
        int rows = featureDesc.rows();
        for (int i = 0; i < rows; i++) {
            featureDesc.rows(i).asCvMat().get(buf);
            // compress data
            float[] siftFeat = new float[buf.length];
            for (int j = 0; j < buf.length; j++) {
                siftFeat[j] = (float) buf[j];
            }
            if (selected.stream().noneMatch(v -> distance(v, siftFeat) < prefiterDist)) {
                selected.add(siftFeat);
            }
        }
        String frameId = input.getStringByField(FIELD_FRAME_ID);
        collector.emit(STREAM_FEATURE_DESC, input, new Values(frameId, selected));
        collector.emit(STREAM_FEATURE_COUNT, input, new Values(frameId, selected.size()));
        collector.ack(input);
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
        declarer.declareStream(STREAM_FEATURE_DESC, new Fields(FIELD_FRAME_ID, FIELD_FEATURE_DESC));
        declarer.declareStream(STREAM_FEATURE_COUNT, new Fields(FIELD_FRAME_ID, FIELD_FEATURE_CNT));
    }
}
