package flink.app;

import flink.model.VideoOrder;
import flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans02TumbWindowApp
 * @description:
 * @author: huJie
 * @create: 2022-10-12 15:57
 **/
public class FlinkTrans02CountWindowApp {
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
        //分组后的组内数据超过5个则触发
//        DataStream<VideoOrder> moneyDS = keyedStream.countWindow(5).sum("money");
        //分组后的组内数据超过3个则触发统计过去的5个数据
        DataStream<VideoOrder> moneyDS = keyedStream.countWindow(5, 3).sum("money");

        moneyDS.print();

        environment.execute("FlinkTrans02CountWindowApp");

    }
}
