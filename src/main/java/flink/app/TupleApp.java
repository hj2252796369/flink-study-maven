package flink.app;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @program: flink-study
 * @ClassName TupleApp
 * @description:
 * @author: huJie
 * @create: 2022-10-07 17:40
 **/
public class TupleApp {
    public static void main(String[] args) {

//        testTuple();

        testMap();
        testFlatMap();

    }


    public static void testTuple() {
        Tuple3 tuple3 = Tuple3.of(1, "xdclass", 122L);
        System.out.println(tuple3.f0);
        System.out.println(tuple3.f1);
        System.out.println(tuple3.f2);
    }

    public static void testMap(){
        List<String> list1 = new ArrayList<>();
        list1.add("springboot,springcloud");
        list1.add("redis6,docker");
        list1.add("kafka,rabbitmq");

        List<String> collect = list1.stream().map(obj -> {
            obj = "前缀添加" + obj;
            return obj;
        }).collect(Collectors.toList());
        System.out.println(collect);
    }

    public static void testFlatMap(){
        List<String> list1 = new ArrayList<>();
        list1.add("springboot,springcloud");
        list1.add("redis6,docker");
        list1.add("kafka,rabbitmq");

        List<String> collect = list1.stream().flatMap(obj -> {
            return Arrays.stream(obj.split(","));
        }).collect(Collectors.toList());
        System.out.println(collect);
    }
}
