package flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @program: flink-study
 * @ClassName Flink01App
 * @description:
 * @author: huJie
 * @create: 2022-10-07 18:19
 **/
public class Flink03SocketApp {
    public static void main(String[] args) throws Exception {
//构建执行任务环境以及任务的启动的入口, 存储全局相关的参数
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();\
        // 启动携带WebUI
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStream<String> stringDS = env.socketTextStream("127.0.0.1", 8888);
        stringDS.print("开始处理");
        // FlatMapFunction<String, String>, key是输入类型，value是Collector响应的收集的类型，看源码注释，也是 DataStream<String>里面泛型类型
        DataStream<String> dataStream = stringDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String val : split) {
                    out.collect(val);
                }
            }
        });
        dataStream.print("结果");
        //DataStream需要调用execute,可以取个名称
        env.execute("data stream job");
    }
}
