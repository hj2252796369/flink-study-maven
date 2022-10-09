package flink.app;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study
 * @ClassName Flink01App
 * @description:
 * @author: huJie
 * @create: 2022-10-07 18:19
 **/
public class FlinkSource03App {
    public static void main(String[] args) throws Exception {
        //构建执行任务环境以及任务的启动的入口, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stringDS = env.socketTextStream("127.0.0.1", 8888);

        stringDS.print("开始处理");
        //DataStream需要调用execute,可以取个名称
        env.execute("data stream job");
    }
}
