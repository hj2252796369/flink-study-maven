package flink.app;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: flink-study-maven
 * @ClassName FlinkSource02
 * @description:
 * @author: huJie
 * @create: 2022-10-09 21:15
 **/
public class FlinkSource02App {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stringDataStreamSource = environment.readTextFile("E:\\SourceCode\\flink-study-maven\\src\\main\\resources\\test.txt", "UTF-8");
        stringDataStreamSource.print("文件中获取数据1");

        environment.execute("FlinkSource02");

    }
}
