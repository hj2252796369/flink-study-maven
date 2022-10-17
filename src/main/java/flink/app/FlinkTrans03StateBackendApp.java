package flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @program: flink-study-maven
 * @ClassName FlinkTrans03StateBackendApp
 * @description:
 * @author: huJie
 * @create: 2022-10-17 18:14
 **/
public class FlinkTrans03StateBackendApp {
    public static void main(String[] args) throws Exception {
        //构建执行任务环境以及任务的启动的入口, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());

        //env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
        //env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));
        //作业单独配置checkpoints
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints-data/");
        //java,2022-11-11 09-10-10,15
        DataStream<String> ds = env.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> flatMapDS = ds.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));

            }
        });

        flatMapDS.print();
        env.execute("watermark job");
    }
}
