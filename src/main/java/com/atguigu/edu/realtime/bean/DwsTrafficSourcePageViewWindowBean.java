package com.atguigu.edu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * description:
 * Created by 铁盾 on 2022/6/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTrafficSourcePageViewWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 引流来源 ID
    String sourceId;

    // 引流来源名称
    String sourceName;

    // 独立访客数
    Long uvCt;

    // 时间戳
    Long ts;

}
