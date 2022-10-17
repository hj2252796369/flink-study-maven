package flink.app;

import flink.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans06CEPApp
 * @description:
 * @author: huJie
 * @create: 2022-10-17 20:51
 **/
public class FlinkTrans06CEPApp {
    public static void main(String[] args) throws Exception {
        // 获取流环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);
        DataStream<String> dataStream = environment.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> flatMap = dataStream.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                out.collect(Tuple3.of(split[0], split[1], Integer.valueOf(split[2])));
            }
        });

        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = flatMap.assignTimestampsAndWatermarks(WatermarkStrategy
                //延迟策略去掉了延迟时间,时间是单调递增，event中的时间戳充当了水印
                .<Tuple3<String, String, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner(((element, recordTimestamp) -> {
                    return TimeUtil.strToDate(element.f1).getTime();
                }))).keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        Pattern<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> pattern =
                Pattern.<Tuple3<String, String, Integer>>begin("firstTimeLogin").where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f2 == -1;
                    }
                }).next("secondTimeLogin").where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f2 == -1;
                    }
                }).within(Time.seconds(5));

        PatternStream<Tuple3<String, String, Integer>> patternStream = CEP.pattern(keyedStream, pattern);

        SingleOutputStreamOperator<Tuple3<String, String, String>> streamOperator = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> select(Map<String, List<Tuple3<String, String, Integer>>> map) throws Exception {

                Tuple3<String, String, Integer> firstTimeLogin = map.get("firstTimeLogin").get(0);
                Tuple3<String, String, Integer> secondTimeLogin = map.get("secondTimeLogin").get(0);

                return Tuple3.of(firstTimeLogin.f0, firstTimeLogin.f1, secondTimeLogin.f1);
            }
        });

        streamOperator.print("打印结果");

        environment.execute("CEP job");

    }
}
