package flink.app;

import flink.model.VideoOrder;
import flink.sink.MyRedisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Date;

/**
 * @program: flink-study-maven
 * @ClassName FlinkSink02App
 * @description:
 * @author: huJie
 * @create: 2022-10-10 15:45
 **/
public class FlinkSink03App {
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

        DataStream<Tuple2<String, Integer>> mapDS = dataStream.map(new MapFunction<VideoOrder, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(VideoOrder value) throws Exception {
                return new Tuple2<>(value.getTitle(), 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        DataStream<Tuple2<String, Integer>> sumDS = keyedStream.sum(1);
        sumDS.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPassword("hj19961017").build();
        sumDS.addSink(new RedisSink<>(conf, new MyRedisSink()));

        environment.execute("数据库存储");
    }
}
