package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * description:
 * Created by 铁盾 on 2022/6/23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeSubjectUserOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 课程 ID
    @TransientSink
    String courseId;

    // 科目 ID
    String subjectId;

    // 科目名称
    String subjectName;

    // 类别 ID
    String categoryId;

    // 类别名称
    String categoryName;

    // 用户 ID
    String userId;

    // 订单 ID
    @TransientSink
    String orderId;

    // 订单数
    Long orderCount;

    // 下单金额
    BigDecimal orderTotalAmount;

    // 时间戳
    Long ts;
}
