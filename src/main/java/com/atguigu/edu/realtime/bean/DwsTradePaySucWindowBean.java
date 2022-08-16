package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradePaySucWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 支付成功独立用户数
    Long paySucUvCount;

    // 支付成功新用户数
    Long paySucNewUserCount;

    // 时间戳
    Long ts;
}
