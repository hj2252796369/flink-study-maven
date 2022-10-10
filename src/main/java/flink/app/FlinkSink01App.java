package flink.app;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study-maven
 * @ClassName FlinkSink01App
 * @description:
 * @author: huJie
 * @create: 2022-10-10 15:25
 **/
public class FlinkSink01App {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = environment.fromElements("springboot, redis", "springcloud, kafka", "alibaba cloud, rabbitmq");
        stringDataStreamSource.print("stringDataStreamSource:");

        environment.execute("从元素集合中获取");
    }
}
