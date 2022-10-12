package flink.app;

import flink.model.VideoOrder;
import flink.source.VideoOrderSourceV2;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans02TumbWindowApp
 * @description:
 * @author: huJie
 * @create: 2022-10-12 15:57
 **/
public class FlinkTrans02ProcessWindowsApp {
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
                        .process(new ProcessWindowFunction<VideoOrder, VideoOrder, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<VideoOrder, VideoOrder, String, TimeWindow>.Context context, Iterable<VideoOrder> elements, Collector<VideoOrder> out) throws Exception {
                                List<VideoOrder> list = IteratorUtils.toList(elements.iterator());
                                int total =list.stream().collect(Collectors.summingInt(VideoOrder::getMoney)).intValue();

                                VideoOrder videoOrder = new VideoOrder();
                                videoOrder.setMoney(total);
                                videoOrder.setTitle(list.get(0).getTitle());
                                videoOrder.setCreateTime(list.get(0).getCreateTime());

                                out.collect(videoOrder);
                            }
                        });

        moneyDS.print();

        environment.execute("FlinkTrans02ProcessWindowsApp");

    }
}
