package flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
}
