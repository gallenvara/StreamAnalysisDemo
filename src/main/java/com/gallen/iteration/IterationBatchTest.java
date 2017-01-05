package com.gallen.iteration;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by gallenvara on 17/1/5.
 */
public class IterationBatchTest {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        IterativeDataSet<Integer> iteration = env
                .fromElements(1)
                .iterate(10);//define iteration times

        DataSet<Integer> result = iteration.closeWith(iteration.map(new AddSuperstepNumberMapper()));

        List<Integer> collected = new ArrayList<Integer>();
        result.print();
        result.output(new LocalCollectionOutputFormat<Integer>(collected));

        env.execute();

        assertEquals (1, collected.size());
        assertEquals(56, collected.get(0).intValue());
    }
    public static class AddSuperstepNumberMapper extends RichMapFunction<Integer, Integer> {

        @Override
        public Integer map(Integer value) {
            int superstep = getIterationRuntimeContext().getSuperstepNumber();
            return value + superstep;
        }
    }
}
