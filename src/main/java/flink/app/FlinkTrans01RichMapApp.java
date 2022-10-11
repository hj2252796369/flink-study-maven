package flink.app;

import flink.model.VideoOrder;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class FlinkTrans01RichMapApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
environment.setParallelism(1);
        DataStream<VideoOrder> dataStream = environment.fromElements(new VideoOrder("21312","java",32,5,new Date()),
                new VideoOrder("314","java",32,5,new Date()),
                new VideoOrder("542","springboot",32,5,new Date()),
                new VideoOrder("42","redis",32,5,new Date()),
                new VideoOrder("52","java",32,5,new Date()),
                new VideoOrder("523","redis",32,5,new Date())
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = dataStream.map(new RichMapFunction<VideoOrder, Tuple2<String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("=============open==========");
            }

            @Override
            public void close() throws Exception {
                System.out.println("=============close==========");
            }

            @Override
            public Tuple2<String, Integer> map(VideoOrder value) throws Exception {
                return new Tuple2<>(value.getTitle(), 1);
            }
        });

        mapDS.print();

        environment.execute("操作流map");

    }
}
