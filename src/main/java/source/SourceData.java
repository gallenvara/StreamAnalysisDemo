package source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.BasicConfigurator;

/**
 * Created by gallenvara on 16/11/26.
 */
public class SourceData {
    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Tuple2<Integer, Integer>> source = env.fromElements(new Tuple2<>(1,1), new Tuple2<>(2,2));
        DataSet<Tuple2<Integer, Integer>> result = source.filter((FilterFunction<Tuple2<Integer, Integer>>) value -> value.f0 % 2 == 0);
        result.print();
    }
}
