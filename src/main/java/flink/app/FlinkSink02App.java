package flink.app;

import flink.model.VideoOrder;
import flink.sink.MySQLSink;
import flink.source.VideoOrderSource;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study-maven
 * @ClassName FlinkSink02App
 * @description:
 * @author: huJie
 * @create: 2022-10-10 15:45
 **/
public class FlinkSink02App {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();

        environment.setParallelism(1);
        DataStreamSource<VideoOrder> videoOrderDataStreamSource = environment.addSource(new VideoOrderSource());

        DataStreamSink<VideoOrder> videoOrderDataStreamSink = videoOrderDataStreamSource.addSink(new MySQLSink());

        environment.execute("数据库存储");
    }
}
