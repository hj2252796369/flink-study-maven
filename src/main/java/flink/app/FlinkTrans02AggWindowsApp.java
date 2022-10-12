package flink.app;

import flink.model.VideoOrder;
import flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans02TumbWindowApp
 * @description:
 * @author: huJie
 * @create: 2022-10-12 15:57
 **/
public class FlinkTrans02AggWindowsApp {
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
        // 每5秒统计数据
        DataStream<VideoOrder> moneyDS = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .aggregate(new AggregateFunction<VideoOrder, VideoOrder, VideoOrder>() {

                            @Override
                            public VideoOrder createAccumulator() {
                                VideoOrder videoOrder = new VideoOrder();
                                return videoOrder;
                            }
                            @Override
                            public VideoOrder add(VideoOrder value, VideoOrder accumulator) {
                                accumulator.setMoney(value.getMoney() + accumulator.getMoney());
                                accumulator.setTitle(value.getTitle());
                                if(accumulator.getCreateTime() == null){
                                    accumulator.setCreateTime(value.getCreateTime());
                                }
                                return accumulator;
                            }

                            @Override
                            public VideoOrder getResult(VideoOrder accumulator) {
                                return accumulator;
                            }

                            @Override
                            public VideoOrder merge(VideoOrder a, VideoOrder b) {
                                return null;
                            }
                        });

        moneyDS.print();

        environment.execute("FlinkTrans02TumbWindowApp");

    }
}
