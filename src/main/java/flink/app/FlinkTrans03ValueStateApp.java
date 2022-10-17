package flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans03ValueStateApp
 * @description:
 * @author: huJie
 * @create: 2022-10-17 18:02
 **/
public class FlinkTrans03ValueStateApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        DataStreamSource<String> dataStreamSource = environment.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> total = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));
            }
        }).keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        }).map(new RichMapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {

            private ValueState<Integer> valueState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("total", Integer.class));
            }

            @Override
            public Tuple2<String, Integer> map(Tuple3<String, String, Integer> value) throws Exception {

                Integer stateMaxValue = valueState.value();

                Integer currentValue = value.f2;

                if (stateMaxValue == null || stateMaxValue < currentValue) {
                    valueState.update(currentValue);
                    return Tuple2.of(value.f0, currentValue);
                } else {
                    //历史值更⼤
                    return Tuple2.of(value.f0, stateMaxValue);
                }
            }
        });

        total.print("获取最大值");

        environment.execute("valueState Execute");
    }
}
