package flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @program: flink-study-maven
 * @ClassName RedisSink
 * @description:
 * @author: huJie
 * @create: 2022-10-10 16:12
 **/
public class MyRedisSink implements RedisMapper<Tuple2<String, Integer>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "VIDEO_ORDER_COUNTER");
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> stringIntegerTuple2) {
        return stringIntegerTuple2.f1.toString();
    }
}
