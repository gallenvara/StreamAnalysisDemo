package com.gallen.source;

import java.io.*;
import java.util.Random;

/**
 * Created by gallenvara on 17/1/19.
 */
public class DataGenerator {
    public static void main(String[] args) {

        Random random = new Random();
        String string = "abcdefghijklmnopqrstuvwxyz";
        String lujing = "/Users/gallenvara/data.txt";
        File file = new File(lujing);
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FileWriter fw = new FileWriter(file, false);
            BufferedWriter bw = new BufferedWriter(fw);
            for (int i = 0; i < 10000; i++) {
                int c1 = random.nextInt(20);
                char c2 = string.charAt(random.nextInt(25));
                int c3 = random.nextInt(5);
                int c4 = random.nextInt(5);
                String result = String.valueOf(c1) +","+ String.valueOf(c2)+"," + String.valueOf(c3)+"," + String.valueOf(c4);
                bw.write(result);
                bw.newLine();
            }
            bw.flush();
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
