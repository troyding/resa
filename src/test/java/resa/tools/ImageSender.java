package resa.tools;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import redis.clients.jedis.Jedis;
import resa.util.ConfigUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.bytedeco.javacpp.opencv_highgui.cvSaveImage;

/**
 * Created by ding on 14-3-18.
 */
public class ImageSender {

    private static final File END = new File("END");

    private String host;
    private int port;
    private byte[] queueName;
    private BlockingQueue<File> dataQueue = new ArrayBlockingQueue<>(10000);

    public ImageSender(Map<String, Object> conf) {
        this.host = (String) conf.get("redis.host");
        this.port = ((Number) conf.get("redis.port")).intValue();
        this.queueName = ((String) conf.get("redis.queue")).getBytes();
    }

    public void send2Queue(String videoFile, int fps, int retain) throws IOException {
        List<Integer> array = new ArrayList(IntStream.range(0, fps).boxed().collect(Collectors.toList()));
        new PushThread().start();
        opencv_highgui.VideoCapture capture = new opencv_highgui.VideoCapture(videoFile);
        try {
            opencv_core.Mat mat = new opencv_core.Mat();
//            opencv_core.IplImage img = null;
            long now;
            Set<Integer> retainFrames = new HashSet<>();
            while (true) {
                now = System.currentTimeMillis();
                retainFrames.clear();
                Collections.shuffle(array);
                for (int j = 0; j < array.size(); j++) {
                    retainFrames.add(array.get(j));
                }
                for (int j = 0; j < retain; j++) {
                    if (!capture.read(mat)) {
                        return;
                    }
                    if (retainFrames.contains(j)) {
                        opencv_core.IplImage source = mat.asIplImage();
//                        if (img == null) {
//                            opencv_core.CvSize size = cvSize((int) (source.width() / 1.5),
//                                    (int) (source.height() / 1.5));
//                            img = opencv_core.IplImage.create(size, source.depth(), source.nChannels());
//                        }
//                        cvResize(source, img);
                        File imgFile = File.createTempFile("img-", ".jpg");
                        cvSaveImage(imgFile.getAbsolutePath(), source);
                        dataQueue.put(imgFile);
                    }
                }
                long sleep = now + 1000 - System.currentTimeMillis();
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            capture.release();
        }
    }

    private class PushThread extends Thread {

        private Jedis jedis = new Jedis(host, port);

        private PushThread() {
        }

        @Override
        public void run() {
            File f;
            try {
                while ((f = dataQueue.take()) != END) {
                    try {
                        jedis.rpush(queueName, Files.readAllBytes(f.toPath()));
                    } catch (IOException e) {
                    } finally {
                        f.delete();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                dataQueue.offer(END);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("usage: ImageSender <confFile> <videoFile> <fps> <retain>");
            return;
        }
        ImageSender sender = new ImageSender(ConfigUtil.readConfig(new File(args[0])));
        if (!new File(args[1]).exists()) {
            throw new FileNotFoundException(args[1]);
        }
        System.out.println("start sender");
        sender.send2Queue(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        System.out.println("end sender");
    }

}
