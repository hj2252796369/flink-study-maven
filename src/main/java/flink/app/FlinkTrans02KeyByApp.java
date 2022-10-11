package flink.app;

import flink.model.VideoOrder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans01MapApp
 * @description:
 * @author: huJie
 * @create: 2022-10-11 16:41
 **/
public class FlinkTrans02KeyByApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        environment.setParallelism(1);
        DataStream<VideoOrder> dataStream = environment.fromElements(new VideoOrder("21312", "java", 32, 5, new Date()),
                new VideoOrder("314", "java", 32, 5, new Date()),
                new VideoOrder("542", "springboot", 32, 5, new Date()),
                new VideoOrder("42", "redis", 32, 5, new Date()),
                new VideoOrder("52", "java", 32, 5, new Date()),
                new VideoOrder("523", "redis", 32, 5, new Date())
        );

        KeyedStream<VideoOrder, String> videoOrderStringKeyedStream = dataStream.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                return value.getTitle();
            }
        });

        SingleOutputStreamOperator<VideoOrder> money = videoOrderStringKeyedStream.sum("money");

        money.print();

        environment.execute("操作流map");

    }
}
