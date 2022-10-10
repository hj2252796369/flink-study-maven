package flink.sink;

import flink.model.VideoOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @program: flink-study-maven
 * @ClassName MySQLSink
 * @description:
 * @author: huJie
 * @create: 2022-10-10 15:30
 **/
public class MySQLSink extends RichSinkFunction<VideoOrder> {
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/xd_order?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&serverTimezone=Asia/Shanghai", "root", "hj19961017");
        String sql = "INSERT INTO `video_order` (`user_id`, `money`, `title`, `trade_no`, `create_time`) VALUES(?,?,?,?,?);";
        preparedStatement = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
    }

    @Override
    public void invoke(VideoOrder videoOrder, Context context) throws Exception {
        //给ps中的?设置具体值
        preparedStatement.setInt(1,videoOrder.getUserId());
        preparedStatement.setInt(2,videoOrder.getMoney());
        preparedStatement.setString(3,videoOrder.getTitle());
        preparedStatement.setString(4,videoOrder.getTradeNo());
        preparedStatement.setDate(5,new Date(videoOrder.getCreateTime().getTime()));
        preparedStatement.executeUpdate();
    }
}
