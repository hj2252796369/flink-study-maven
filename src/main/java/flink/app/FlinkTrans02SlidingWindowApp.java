package flink.app;

import flink.model.VideoOrder;
import flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans02TumbWindowApp
 * @description:
 * @author: huJie
 * @create: 2022-10-12 15:57
 **/
public class FlinkTrans02SlidingWindowApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        DataStream<VideoOrder> dataStream = environment.addSource(new VideoOrderSourceV2());

        KeyedStream<VideoOrder, String> keyedStream = dataStream.keyBy(new KeySelector<VideoOrder, String>() {

            @Override
            public String getKey(VideoOrder value) throws Exception {
                return value.getTitle();
            }
        });
        // 每3秒统计过去10秒的数据
        DataStream<VideoOrder> moneyDS = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(3))).sum("money");


        moneyDS.print();


        environment.execute("FlinkTrans02TumbWindowApp");

    }
}
