package resa.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.StringTokenizer;

/**
 * Created by ding on 14/10/20.
 */
public class WordCounter {

    public static void main(String[] args) throws IOException {
        Files.lines(Paths.get(args[0])).mapToInt(line -> {
            StringTokenizer tokenizer = new StringTokenizer(line.replaceAll("\\p{P}|\\p{S}", " "));
            int count = 0;
            while (tokenizer.hasMoreTokens()) {
                count++;
            }
            return count;
        }).forEach(System.out::println);
    }

}
