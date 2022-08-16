package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 下单独立用户数
    Long orderUvCount;

    // 新增下单用户数
    Long newOrderUserCount;

    // 时间戳
    Long ts;
}
