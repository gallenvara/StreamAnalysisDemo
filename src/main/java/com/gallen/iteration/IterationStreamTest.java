package com.gallen.iteration;

import com.gallen.source.StreamSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by gallenvara on 17/1/5.
 */

public class IterationStreamTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        sEnv.setParallelism(1);
        sEnv.setBufferTimeout(1);
        IterativeStream<Integer> iterativeStream = sEnv
                .addSource(new StreamSourceFunction())
                .iterate(2000);//unit: ms
                               //max wait time for a stream iteration,
                               //not same with batch iteration,
                               //no times concept.
        DataStream<Integer> dataStream = iterativeStream
                .closeWith(iterativeStream
                        .map(new MyStreamIterationMap()));

        dataStream.print();
        sEnv.execute("streaming iteration test");

    }
    public static class MyStreamIterationMap extends RichMapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer value) {
            //int superStep = getIterationRuntimeContext().getSuperstepNumber();
            //throw exception
            int superStep = 2;
            return value + superStep;
        }
    }
}