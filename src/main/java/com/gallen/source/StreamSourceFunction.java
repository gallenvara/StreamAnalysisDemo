package com.gallen.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Created by gallenvara on 17/1/5.
 */
public class StreamSourceFunction implements SourceFunction<Integer> {
    private volatile boolean isRunning = true;
    private Random random = new Random();
    private int count = 0;
    public void run(SourceContext<Integer> sourceContext) throws InterruptedException{
        while (isRunning) {
            count++;//count the times for every iteration value
            int value = random.nextInt(10000);
            sourceContext.collect(value);
            //Thread.sleep(100);
            //Thread.sleep(5);
        }
    }
    public void cancel() {
        isRunning = false;
    }
}

