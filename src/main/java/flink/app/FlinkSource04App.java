package flink.app;

import flink.model.VideoOrder;
import flink.source.VideoOrderSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study-maven
 * @ClassName FlinkSource04
 * @description:
 * @author: huJie
 * @create: 2022-10-09 21:18
 **/
public class FlinkSource04App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        environment.setParallelism(8);
        DataStreamSource<VideoOrder> videoOrderDataStreamSource = environment.addSource(new VideoOrderSource());
        videoOrderDataStreamSource.print();

        environment.execute("richsource");
    }
}
