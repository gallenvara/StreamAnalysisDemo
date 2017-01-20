package com.gallen.cardinality;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;

/**
 * Created by gallenvara on 17/1/20.
 */

public class TestCardinality {

    private static Long count = 0l;

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        // 计算基数
        ICardinality c1 = cardinality("/Users/gallenvara/MyGit/testData/ip1.txt");
        ICardinality c2 = cardinality("/Users/gallenvara/MyGit/testData/ip2.txt");
        ICardinality c3 = cardinality("/Users/gallenvara/MyGit/testData/ip3.txt");

        System.out.println("c1 unique count:" + c1.cardinality());
        System.out.println("c2 unique count:" + c2.cardinality());
        System.out.println("c3 unique count:" + c3.cardinality());

        /*
         * 合并三个数据集
         */
        try {
            System.out.println("c1 c2 c3 merge unique count:" + c1.merge(c2).merge(c3).cardinality());
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }

        System.out.println("Total count:" + count);
        System.out.println("Total cost:" + (System.currentTimeMillis() - start) + " ms ..");
        // System.out.println(Runtime.getRuntime().freeMemory());

    }

    /**
     * 估算uv
     *
     * @param filePath 文件路径
     * @return
     * @throws FileNotFoundException
     */
    public static ICardinality cardinality(String filePath) throws IOException {

        // 初始化
        ICardinality card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();

        /*
         * 读取文件并添加到card
         */
        File file = new File(filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                card.offer(tempString);
                count++;
            }

        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return card;
    }
}
