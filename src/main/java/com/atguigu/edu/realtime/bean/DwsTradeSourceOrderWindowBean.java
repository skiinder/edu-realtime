package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class DwsTradeSourceOrderWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 来源 ID
    String sourceId;

    // 来源名称
    String sourceName;

    // 订单 ID
    @TransientSink
    String orderId;

    // 交易总额
    BigDecimal orderTotalAmount;

    // 下单独立用户数
    Long orderUuCount;

    // 订单数
    Long orderCount;

    // 时间戳
    Long ts;
}
