package flink.app;

import flink.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans03WatermarkWindwoApp
 * @description:
 * @author: huJie
 * @create: 2022-10-17 15:18
 **/
public class FlinkTrans03WatermarkWindwoApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);
        DataStreamSource<String> dataStreamSource = environment.socketTextStream("127.0.0.1", 18888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> flatMapDS = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                Tuple3<String, String, Integer> record = new Tuple3<>();
                String[] split = value.split(",");
                record.setFields(split[0], split[1], Integer.valueOf(split[2]));
                out.collect(record);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> watermakerDS = flatMapDS.assignTimestampsAndWatermarks(WatermarkStrategy
                // 允许最长延迟
                .<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(((element, recordTimestamp) -> {
                    // 指定POJO的事件时间
                    return TimeUtil.strToDate(element.f1).getTime();
                })));
        // 对数据进行开窗处理
        SingleOutputStreamOperator<String> sunDS = watermakerDS.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {

            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple3<String, String, Integer>, String, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Integer>> input, Collector<String> out) throws Exception {
                //存放窗⼝的数据的事件时间
                List<String> eventTimeList = new ArrayList<>();
                int total = 0;
                for (Tuple3<String, String, Integer>
                        order : input) {
                    eventTimeList.add(order.f1);
                    total = total + order.f2;
                }
                String outStr = String.format("分组key:%s, 聚合值:%s, 窗⼝开始结束:[%s ~ %s),窗⼝所有事件时 间:%s ", key, total, TimeUtil.format(window.getStart()), TimeUtil.format(window.getEnd()), eventTimeList);
                out.collect(outStr);
            }
        });

        sunDS.print("数据：");

        environment.execute();


    }
}
