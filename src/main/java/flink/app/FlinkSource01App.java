package flink.app;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @program: flink-study-maven
 * @ClassName FlinkSource01
 * @description:
 * @author: huJie
 * @create: 2022-10-09 21:10
 **/
public class FlinkSource01App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = environment.fromElements("springboot, redis", "springcloud, kafka", "alibaba cloud, rabbitmq");
        stringDataStreamSource.print("stringDataStreamSource:");

        DataStreamSource<String> stringDataStreamSource1 = environment.fromCollection(Arrays.asList("springboot, redis", "springcloud, kafka", "alibaba cloud, rabbitmq"));
        stringDataStreamSource1.print("stringDataStreamSource1");


        DataStreamSource<Long> longDataStreamSource = environment.fromSequence(1L, 20L);
        longDataStreamSource.print("longDataStreamSource").setParallelism(1);

        environment.execute("从元素集合中获取");
    }
}
