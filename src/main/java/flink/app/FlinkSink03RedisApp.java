package flink.app;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @program: flink-study-maven
 * @ClassName FinkSink03RedisApp
 * @description:
 * @author: huJie
 * @create: 2022-10-10 17:52
 **/
public class FlinkSink03RedisApp {
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "101.33.225.56:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "video-order-group");
        //字符串序列化和反序列化规则
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
        //有后台线程每隔10s检测一下Kafka的分区变化情况
        properties.setProperty("flink.partition-discovery.interval-millis","10000");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("xdclass-topic", new SimpleStringSchema(), properties);
        //设置从记录的消费者组内的offset开始消费
        consumer.setStartFromGroupOffsets();

        DataStreamSource<String> dataStreamSource = environment.addSource(consumer);
        dataStreamSource.print("kafka");
    }
}
