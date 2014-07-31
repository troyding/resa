package resa.topology.fp;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by ding on 14-6-5.
 */
public class PatternGenerator extends BaseRichBolt implements Constant {

    private OutputCollector collector;
    private Set<String> words;
    private Map<String, Integer> dict;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        words = new HashSet<>();
        dict = new HashMap<>();
        int id = 0;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream("/dict-100000.txt")))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                dict.put(line, id++);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getStringByField(SENTENCE_FIELD);
        StringTokenizer tokenizer = new StringTokenizer(sentence.replaceAll("\\p{P}|\\p{S}", " "));
        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            words.add(word);
        }
        int[] wordIds = words.stream().map(this::word2Id).filter(Objects::nonNull).mapToInt(i -> i).sorted().toArray();
        if (wordIds.length > 0) {
            emitSubPattern(wordIds, collector, input);
        }
        words.clear();
        collector.ack(input);
    }

    private void emitSubPattern(int[] wordIds, OutputCollector collector, Tuple input) {
        int n = wordIds.length;
        int[] buffer = new int[n];
        for (int i = 1; i < (1 << n); i++) {
            int k = 0;
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    buffer[k++] = wordIds[j];
                }
            }
            collector.emit(input, Arrays.asList(new WordList(Arrays.copyOf(buffer, k)),
                    input.getValueByField(IS_ADD_FIELD)));
        }
    }

    private Integer word2Id(String word) {
        return dict.get(word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PATTERN_FIELD, IS_ADD_FIELD));
    }
}
