package flink.app;

import flink.model.VideoOrder;
import flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans01MapApp
 * @description:
 * @author: huJie
 * @create: 2022-10-11 16:41
 **/
public class FlinkTrans02ReduceApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        environment.setParallelism(1);
        DataStream<VideoOrder> dataStream = environment.addSource(new VideoOrderSourceV2());

        DataStream<VideoOrder> filterDS = dataStream.filter(new FilterFunction<VideoOrder>() {
            @Override
            public boolean filter(VideoOrder value) throws Exception {
                return value.getMoney() > 50;
            }
        }).keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                return value.getTitle();
            }
        }).reduce(new ReduceFunction<VideoOrder>() {
            @Override
            public VideoOrder reduce(VideoOrder value1, VideoOrder value2) throws Exception {
                VideoOrder videoOrder = new VideoOrder();
                videoOrder.setTitle(value1.getTitle());
                videoOrder.setMoney(value1.getMoney() + value2.getMoney());
                return videoOrder;
            }
        });

        filterDS.print();

        environment.execute("操作流map");

    }
}
