package com.gallen.stream;

import com.gallen.source.StreamSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.SumFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.*;

import java.io.Serializable;
import java.lang.String;
import java.util.*;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by gallenvara on 17/1/16.
 */
public class CoMapStream {

    private static Random random = new Random();
    private static int rangeSize = 9;
    private static PriorityBlockingQueue<Tuple2<Double, Integer>> rangeQueue = new PriorityBlockingQueue<>(rangeSize, new Comparator<Tuple2<Double, Integer>>() {
        @Override
        public int compare(Tuple2<Double, Integer> o1, Tuple2<Double, Integer> o2) {
            return o1.f0 - o2.f0 > 0 ? 1 : -1;
        }
    });
    public static void main(java.lang.String[] args) throws Exception{

        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setParallelism(1);
        DataStream<Integer> source1 = sEnv
                .addSource(new StreamSourceFunction());
        IterativeStream<Integer> learningStream = source1
                .iterate(500);
        DataStream<Integer> result =
                source1
                .connect(learningStream.closeWith(learningStream.map(new MyMapFunction())))
                .flatMap(new CoFlatMapFunction<Integer, Integer, Integer>() {
                    @Override
                    public void flatMap1(Integer value, Collector<Integer> out) throws Exception {
                        System.out.println("current range queue size is : " + rangeQueue.size());
                        List<Integer> list = new ArrayList<Integer>();

                        for (Tuple2<Double, Integer> element : rangeQueue) {
                            list.add(element.f1);
                        }
                        list.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o1 - o2;
                            }
                        });
                        System.out.println("list size : " + list.size());
                        int index = -1;
                        System.out.print("queue element : ");
                        if (list.size() > 0) {
                            Object[] elementArr = list.toArray();
                            if (value < (Integer) elementArr[0]) index = 0;
                            if (value >= (Integer) elementArr[elementArr.length - 1]) index = elementArr.length;
                            for (int i = 0; i < elementArr.length - 1; i++) {
                                if (value >= (Integer) elementArr[i] && value < (Integer) elementArr[i + 1]) {
                                    index = i + 1;
                                }
                            }
                        }
                        for (Integer element : list) {
                            System.out.print(element + " ");
                        }
                        System.out.println();
                        System.out.println("current waited predicted value is : "
                                + value
                                + ", and it should be emitted to "
                                + index + " range.");
                        out.collect(value);
                    }

                    @Override
                    public void flatMap2(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(-1);
                    }
                });

        //result.print();
        sEnv.execute();
    }
    public static class MyMapFunction extends RichMapFunction<Integer, Integer> implements Serializable {
        @Override
        public Integer map(Integer value) {
            if (value != -1) {
                double rand = random.nextDouble();
                Tuple2<Double, Integer> element = new Tuple2<Double, Integer>(rand, value);
                if (rangeQueue.size() < rangeSize) {
                    rangeQueue.offer(element);
                } else {
                    if (rand > rangeQueue.peek().f0) {
                        rangeQueue.remove();
                        rangeQueue.offer(element);
                    }
                }
            }
            return -1;
        }
    }
}
