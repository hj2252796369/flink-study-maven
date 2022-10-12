package flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoOrder {
    private String tradeNo;
    private String title;
    private int money;
    private int userId;
    private Date createTime;

    @Override
    public String toString() {
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneId = ZoneId.systemDefault();
        return "VideoOrder{" +
                "tradeNo='" + tradeNo + '\'' +
                ", title='" + title + '\'' +
                ", money=" + money +
                ", userId=" + userId +
                ", createTime=" +
                formatter.format(createTime.toInstant().atZone(zoneId)) + '}';
    }

}
