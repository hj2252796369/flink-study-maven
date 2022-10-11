package flink.app;

import flink.model.VideoOrder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans01MapApp
 * @description:
 * @author: huJie
 * @create: 2022-10-11 16:41
 **/
public class FlinkTrans01FlatMapApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<VideoOrder> dataStream = environment.fromElements(new VideoOrder("21312","java",32,5,new Date()),
                new VideoOrder("314","java",32,5,new Date()),
                new VideoOrder("542","springboot",32,5,new Date()),
                new VideoOrder("42","redis",32,5,new Date()),
                new VideoOrder("52","java",32,5,new Date()),
                new VideoOrder("523","redis",32,5,new Date())
        );

        SingleOutputStreamOperator<String> streamOperator = dataStream.flatMap(new FlatMapFunction<VideoOrder, String>() {
            @Override
            public void flatMap(VideoOrder value, Collector<String> out) throws Exception {
                out.collect(value.getTitle());
            }
        });
        streamOperator.print();

        environment.execute("操作流map");

    }
}
