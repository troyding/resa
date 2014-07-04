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

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        sift = new SIFT();
        buf = new double[128];
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
        int rows = featureDesc.rows();
        for (int i = 0; i < rows; i++) {
            featureDesc.rows(i).asCvMat().get(buf);
            // compress data
            float[] siftFeat = new float[buf.length];
            for (int j = 0; j < buf.length; j++) {
                siftFeat[j] = (float) buf[j];
            }
            Point2f p = points.pt();
            collector.emit(STREAM_FEATURE_DESC, input,
                    new Values(input.getValueByField(FIELD_FRAME_ID), p.x(), p.y(), siftFeat));
        }
        collector.emit(STREAM_FEATURE_COUNT, input, new Values(input.getValueByField(FIELD_FRAME_ID), rows));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_FEATURE_DESC,
                new Fields(FIELD_FRAME_ID, FIELD_POINT_X, FIELD_POINT_Y, FIELD_FEATURE_DESC));
        declarer.declareStream(STREAM_FEATURE_COUNT, new Fields(FIELD_FRAME_ID, FIELD_FEATURE_CNT));
    }
}
