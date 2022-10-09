package flink.source;

import flink.model.VideoOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.*;

/**
 * @program: flink-study-maven
 * @ClassName VideoOrderSource
 * @description:
 * @author: huJie
 * @create: 2022-10-09 21:30
 **/
// 并行度等于1
public class VideoOrderSource extends RichSourceFunction<VideoOrder> {
    // 并行度大于1
//public class VideoOrderSource extends RichParallelSourceFunction<VideoOrder> {

    private volatile boolean flag = true;

    private Random random = new Random();

    private static List<String> list = new ArrayList<>();
    static {
        list.add("spring boot2.x课程");
        list.add("微服务SpringCloud课程");
        list.add("RabbitMQ消息队列");
        list.add("Kafka课程");
        list.add("小滴课堂面试专题第一季");
        list.add("Flink流式技术课程");
        list.add("工业级微服务项目大课训练营");
        list.add("Linux课程");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("===============   VideoOrderSource  open   =================");
    }

    @Override
    public void close() throws Exception {
        System.out.println("===============   VideoOrderSource  close   =================");
    }

    @Override
    public void run(SourceContext<VideoOrder> ctx) throws Exception {
        while (flag) {
            Thread.sleep(1000);
            String id = UUID.randomUUID().toString();
            int userId = random.nextInt(10);
            int money = random.nextInt(100);
            int videoNum = random.nextInt(list.size());
            String title = list.get(videoNum);
            ctx.collect(new VideoOrder(id,title,money,userId,new Date()));
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
